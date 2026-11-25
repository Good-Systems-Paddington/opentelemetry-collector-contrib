import contextlib
import http.client
import json
import pathlib
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def wait(check):
    deadline = time.monotonic() + 60
    while True:
        try:
            return check()
        except (OSError, http.client.HTTPException, AssertionError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.25)


@contextlib.contextmanager
def container(*args):
    identifier = docker("run", "-d", *args)
    try:
        yield identifier
    except BaseException:
        subprocess.run(["docker", "logs", identifier], check=False)
        raise
    finally:
        docker("rm", "-f", identifier)


def mapped_port(identifier, port):
    return docker("port", identifier, f"{port}/tcp").rsplit(":", 1)[1]


def verify(image, network, version):
    with container(
        "--network", network, "--network-alias", "clickhouse", "--memory=1g",
        "-p", "127.0.0.1::8123", "-e", "CLICKHOUSE_USER=fixture",
        "-e", "CLICKHOUSE_PASSWORD=fixture",
        "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
        f"clickhouse/clickhouse-server:{version}",
    ) as clickhouse:
        def query(sql):
            request = urllib.request.Request(
                "http://127.0.0.1:" + mapped_port(clickhouse, 8123), sql.encode(),
                headers={"X-ClickHouse-User": "fixture", "X-ClickHouse-Key": "fixture"},
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.read().decode()

        wait(lambda: query("SELECT 1"))
        for schema in ["map", "json"]:
            for protocol, port in [("tcp", 9000), ("http", 8123)]:
                database = f"fixture_{schema}_{protocol}"
                config = {
                    "receivers": {"otlp": {"protocols": {"http": {"endpoint": "0.0.0.0:4318"}}}},
                    "exporters": {"clickhouse": {
                        "endpoint": f"{protocol}://clickhouse:{port}",
                        "username": "fixture", "password": "fixture", "database": database,
                        "create_schema": True, "ttl": "0s",
                        "connection_params": {"enable_json_type": "1", "output_format_native_write_json_as_string": "1"},
                    }},
                    "service": {"pipelines": {"logs": {"receivers": ["otlp"], "exporters": ["clickhouse"]}}},
                }
                with tempfile.TemporaryDirectory() as directory:
                    path = pathlib.Path(directory) / "config.json"
                    path.write_text(json.dumps(config))
                    path.chmod(0o644)
                    with container(
                        "--network", network, "-p", "127.0.0.1::4318",
                        "-v", f"{path}:/config.json:ro", image, "--config=/config.json",
                        f"--feature-gates={'+' if schema == 'json' else '-'}clickhouse.json",
                    ) as collector:
                        endpoint = "http://127.0.0.1:" + mapped_port(collector, 4318) + "/v1/logs"

                        def send(body):
                            request = urllib.request.Request(endpoint, json.dumps(body).encode(), headers={"Content-Type": "application/json"})
                            with urllib.request.urlopen(request, timeout=5) as response:
                                result = json.load(response)
                            assert not result.get("partialSuccess"), result
                            return result

                        wait(lambda: send({"resourceLogs": []}))
                        fixtures = [
                            (1750000000123456789, 1750000000987654321),
                            (0, 1750000000987654321),
                            (1750000000123456789, 0),
                        ]
                        records = []
                        for index, (event, observed) in enumerate(fixtures):
                            record = {"eventName": "browser.web_vital", "body": {"stringValue": str(index)}}
                            if event:
                                record["timeUnixNano"] = str(event)
                            if observed:
                                record["observedTimeUnixNano"] = str(observed)
                            records.append(record)
                        send({"resourceLogs": [{"scopeLogs": [{"logRecords": records}]}]})

                        def read():
                            rows = json.loads(query(
                                "SELECT toString(toUnixTimestamp64Nano(Timestamp)) AS event, "
                                "toString(toUnixTimestamp64Nano(ObservedTimestamp)) AS observed, EventName "
                                f"FROM {database}.otel_logs ORDER BY Body FORMAT JSON"
                            ))["data"]
                            assert len(rows) == len(fixtures), rows
                            return rows

                        rows = wait(read)
                        assert [(int(row["event"]), int(row["observed"]), row["EventName"]) for row in rows] == [
                            (event or observed, observed, "browser.web_vital") for event, observed in fixtures
                        ], rows
                        print(json.dumps({"clickhouse": version, "schema": schema, "protocol": protocol, "rows": rows}), flush=True)


def verify_storage(image):
    config = {
        "extensions": {"file_storage": {
            "directory": "/tmp/queue", "create_directory": True, "fsync": True,
            "compaction": {"directory": "/tmp/compaction", "on_start": True, "on_rebound": True},
        }},
        "receivers": {"otlp": {"protocols": {"http": {"endpoint": "0.0.0.0:4318"}}}},
        "exporters": {"otlphttp": {
            "endpoint": "http://127.0.0.1:1",
            "retry_on_failure": {"enabled": True, "max_elapsed_time": "0s"},
            "sending_queue": {
                "storage": "file_storage", "sizer": "bytes", "queue_size": 16 * 1024 ** 3,
                "wait_for_result": False,
                "batch": {"sizer": "items", "min_size": 10000, "max_size": 0, "flush_timeout": "5s"},
            },
        }},
        "service": {
            "extensions": ["file_storage"],
            "pipelines": {"logs": {"receivers": ["otlp"], "exporters": ["otlphttp"]}},
        },
    }
    with tempfile.TemporaryDirectory() as directory:
        path = pathlib.Path(directory) / "config.json"
        path.write_text(json.dumps(config))
        path.chmod(0o644)
        docker("run", "--rm", "-v", f"{path}:/config.json:ro", image, "validate", "--config=/config.json")
    print(json.dumps({"file_storage": "validated", "queue": "persistent byte-sized with item batching"}), flush=True)


def verify_overflow(image):
    s3 = {"region": "us-west-2", "s3_bucket": "overflow", "s3_prefix": "metrics"}
    gateway = {
        "extensions": {"file_storage": {"directory": "/tmp/queue", "create_directory": True, "fsync": True}},
        "receivers": {"otlp": {"protocols": {"http": {"endpoint": "0.0.0.0:4318"}}}},
        "connectors": {"failover/metrics": {
            "priority_levels": [["metrics/clickhouse"], ["metrics/overflow"]],
            "retry_interval": "1m",
            "sending_queue": {"enabled": False},
        }},
        "exporters": {
            "otlphttp": {
                "endpoint": "http://127.0.0.1:1",
                "retry_on_failure": {"enabled": True, "max_elapsed_time": "0s"},
                "sending_queue": {"storage": "file_storage", "sizer": "bytes", "queue_size": 4096, "wait_for_result": False},
            },
            "awss3": {
                "s3uploader": {**s3, "s3_partition_format": "year=%Y/month=%m/day=%d/hour=%H/minute=%M", "compression": "gzip"},
                "marshaler": "otlp_proto",
                "sending_queue": {"enabled": True, "wait_for_result": True, "sizer": "bytes", "queue_size": 268435456,
                                  "batch": {"sizer": "items", "min_size": 10000, "max_size": 50000, "flush_timeout": "5s"}},
            },
        },
        "service": {
            "extensions": ["file_storage"],
            "pipelines": {
                "metrics/in": {"receivers": ["otlp"], "exporters": ["failover/metrics"]},
                "metrics/clickhouse": {"receivers": ["failover/metrics"], "exporters": ["otlphttp"]},
                "metrics/overflow": {"receivers": ["failover/metrics"], "exporters": ["awss3"]},
            },
        },
    }
    replay = {
        "receivers": {"awss3": {"starttime": "2026-11-24 00:00", "endtime": "2026-11-25 00:00",
                                "s3downloader": {**s3, "s3_partition": "minute"}}},
        "exporters": {"otlphttp": {"endpoint": "http://127.0.0.1:1"}},
        "service": {"pipelines": {"metrics": {"receivers": ["awss3"], "exporters": ["otlphttp"]}}},
    }
    with tempfile.TemporaryDirectory() as directory:
        for name, config in [("gateway", gateway), ("replay", replay)]:
            path = pathlib.Path(directory) / f"{name}.json"
            path.write_text(json.dumps(config))
            path.chmod(0o644)
            docker("run", "--rm", "-v", f"{path}:/config.json:ro", image, "validate", "--config=/config.json")
    print(json.dumps({"overflow": "validated", "replay": "validated"}), flush=True)


def verify_auth(image):
    secret = "fixture-" + uuid.uuid4().hex
    config = {
        "extensions": {"bearertokenauth": {"token": "expected-" + uuid.uuid4().hex}},
        "receivers": {"otlp": {"protocols": {"http": {
            "endpoint": "0.0.0.0:4318", "auth": {"authenticator": "bearertokenauth"},
        }}}},
        "exporters": {"otlphttp": {"endpoint": "http://127.0.0.1:1"}},
        "service": {
            "extensions": ["bearertokenauth"],
            "pipelines": {"logs": {"receivers": ["otlp"], "exporters": ["otlphttp"]}},
        },
    }
    with tempfile.TemporaryDirectory() as directory:
        path = pathlib.Path(directory) / "config.json"
        path.write_text(json.dumps(config))
        path.chmod(0o644)
        with container(
            "-p", "127.0.0.1::4318", "-v", f"{path}:/config.json:ro", image, "--config=/config.json",
        ) as collector:
            endpoint = "http://127.0.0.1:" + mapped_port(collector, 4318) + "/v1/logs"

            def reject(authorization):
                request = urllib.request.Request(endpoint, b"{}", headers={
                    "Content-Type": "application/json", "Authorization": authorization,
                })
                try:
                    urllib.request.urlopen(request, timeout=5)
                except urllib.error.HTTPError as error:
                    body = error.read().decode()
                    assert error.code == 401, (error.code, body)
                    assert secret not in body, body
                    return body
                raise AssertionError("unauthorized request was accepted")

            for authorization in ["Bearer " + secret, secret]:
                body = wait(lambda: reject(authorization))
                print(json.dumps({"auth": "rejected", "body": body}), flush=True)


verify_storage(sys.argv[1])
verify_overflow(sys.argv[1])
verify_auth(sys.argv[1])
network = "gateway-roundtrip-" + uuid.uuid4().hex
docker("network", "create", network)
try:
    for version in ["25.8-alpine", "25.3-alpine"]:
        verify(sys.argv[1], network, version)
finally:
    docker("network", "rm", network)
