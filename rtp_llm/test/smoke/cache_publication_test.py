import ast
import json
import logging
import os
import pathlib
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

from cache_publication import (
    CachePublicationError,
    CachePublicationTimeout,
    CacheStatusSnapshot,
    fetch_cache_status,
    run_cache_publication_pair,
    wait_for_cache_publication,
)


def status(version=1, keys=(10,), available=100, total=100):
    # CacheStatusPB -> MessageToDict(preserving_proto_field_name=True):
    # int64 scalars/map keys are strings, bool map values remain JSON booleans.
    return {
        "results": [
            {
                "version": str(version),
                "cache_keys": {str(key): True for key in keys},
                "available_kv_cache": str(available),
                "total_kv_cache": str(total),
                "address": "worker:1",
            }
        ]
    }


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def wait(self, duration):
        self.now += duration


class CachePublicationTest(unittest.TestCase):
    def test_proto_omitted_zero_version_and_negative_first_published_key(self):
        # Faithful proto3 JSON without default-valued fields. SharedBlockCache
        # starts at -1; its first put advances to 0, which serializers may omit.
        baseline = {
            "results": [
                {
                    "version": "-1",
                    "total_kv_cache": "100",
                    "available_kv_cache": "100",
                    "address": "worker:1",
                }
            ]
        }
        first_publication = {
            "results": [
                {
                    "cache_keys": {"-9223372036854775808": True},
                    "total_kv_cache": "100",
                    "available_kv_cache": "100",
                    "address": "worker:1",
                }
            ]
        }
        samples = iter((baseline, first_publication))
        calls = []
        run_cache_publication_pair(
            lambda: calls.append("producer"),
            lambda: calls.append("consumer"),
            lambda timeout: next(samples),
            0.1,
        )
        self.assertEqual(calls, ["producer", "consumer"])
        published = CacheStatusSnapshot.parse(first_publication)
        self.assertEqual(published.version, 0)
        self.assertEqual(published.keys, frozenset((-(1 << 63),)))

    def test_false_or_deleted_key_cannot_satisfy_new_publication(self):
        current = status(2)
        current["results"][0]["cache_keys"]["-11"] = False
        clock = FakeClock()
        with self.assertRaisesRegex(CachePublicationTimeout, "deadline"):
            wait_for_cache_publication(
                CacheStatusSnapshot.parse(status()),
                lambda timeout: current,
                0.05,
                monotonic=clock.monotonic,
                wait=clock.wait,
            )

    def test_omitted_zero_available_waits_instead_of_failing_or_releasing(self):
        running = status(2, (-11,))
        del running["results"][0]["available_kv_cache"]
        samples = iter((running, status(3, (-11,))))
        clock = FakeClock()
        result = wait_for_cache_publication(
            CacheStatusSnapshot.parse(status()),
            lambda timeout: next(samples),
            0.1,
            monotonic=clock.monotonic,
            wait=clock.wait,
        )
        self.assertEqual(CacheStatusSnapshot.parse(running).available, 0)
        self.assertEqual(result.version, 3)
        self.assertGreater(clock.now, 0)

    def test_cache_keys_require_int64_string_keys_and_real_bool_values(self):
        malformed_maps = (
            {str(-(1 << 63) - 1): True},
            {str(1 << 63): True},
            {"1": "true"},
            {"1": 1},
            {"1": None},
            {"not-a-key": True},
            {1: True},
        )
        for keys in malformed_maps:
            with self.subTest(keys=keys):
                current = status()
                current["results"][0]["cache_keys"] = keys
                with self.assertRaises(CachePublicationError):
                    CacheStatusSnapshot.parse(current)
        limits = CacheStatusSnapshot.parse(status(1, (-(1 << 63), (1 << 63) - 1)))
        self.assertEqual(limits.keys, frozenset((-(1 << 63), (1 << 63) - 1)))

    def test_empty_query_barrier_config_cannot_return_success(self):
        path = pathlib.Path(__file__).resolve().parent / "case_runner.py"
        tree = ast.parse(path.read_text())
        cls = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "CaseRunner"
        )
        method = next(
            node
            for node in cls.body
            if isinstance(node, ast.FunctionDef) and node.name == "_curl_server_impl"
        )

        class SmokeException(Exception):
            def __init__(self, error_status, message):
                self.error_status = error_status
                super().__init__(message)

        namespace = {
            "TaskStates": lambda: SimpleNamespace(ret=True),
            "CachePublicationError": CachePublicationError,
            "SmokeException": SmokeException,
            "QueryStatus": SimpleNamespace(VALID_FAILED="invalid"),
        }
        module = ast.Module(
            body=[
                ast.ImportFrom(
                    module="__future__", names=[ast.alias(name="annotations")], level=0
                ),
                method,
            ],
            type_ignores=[],
        )
        exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)
        task = SimpleNamespace(
            query_result=[], endpoint="/", cache_publication_barrier=object()
        )
        with self.assertRaisesRegex(SmokeException, "exactly one sequential") as error:
            namespace["_curl_server_impl"](object(), object(), task)
        self.assertEqual(error.exception.error_status, "invalid")

    def test_empty_cache_initial_version_minus_one_is_valid(self):
        samples = iter((status(-1, ()), status(0, (11,))))
        calls = []
        result = run_cache_publication_pair(
            lambda: calls.append("producer"),
            lambda: calls.append("consumer"),
            lambda timeout: next(samples),
            0.1,
        )
        self.assertEqual(calls, ["producer", "consumer"])
        self.assertEqual(result, (None, None))

    def test_stale_version_or_preexisting_keys_never_release_consumer(self):
        missing_keys = status(2, ())
        del missing_keys["results"][0]["cache_keys"]
        for current in (status(2), status(1, (10, 11)), status(0, (11,)), missing_keys):
            with self.subTest(current=current):
                clock = FakeClock()
                with self.assertRaisesRegex(CachePublicationTimeout, "deadline"):
                    wait_for_cache_publication(
                        CacheStatusSnapshot.parse(status()),
                        lambda timeout: current,
                        0.05,
                        monotonic=clock.monotonic,
                        wait=clock.wait,
                    )

    def test_new_key_requires_released_running_capacity(self):
        clock = FakeClock()
        samples = iter((status(2, (10, 11), 99), status(3, (10, 11))))
        observed = []

        def fetch(timeout):
            observed.append(timeout)
            return next(samples)

        result = wait_for_cache_publication(
            CacheStatusSnapshot.parse(status()),
            fetch,
            1,
            monotonic=clock.monotonic,
            wait=clock.wait,
        )
        self.assertEqual(result.version, 3)
        self.assertEqual(result.keys, frozenset((10, 11)))
        self.assertEqual(observed, [1, 0.98])

    def test_response_at_deadline_cannot_release_consumer(self):
        clock = FakeClock()

        def fetch(timeout):
            self.assertEqual(timeout, 0.03)
            clock.wait(timeout)
            return status(2, (10, 11))

        with self.assertRaisesRegex(CachePublicationTimeout, "exceeded"):
            wait_for_cache_publication(
                CacheStatusSnapshot.parse(status()),
                fetch,
                0.03,
                monotonic=clock.monotonic,
                wait=clock.wait,
            )

    def test_invalid_status_fails_without_inference(self):
        invalid_samples = (
            {"error": "backend unavailable"},
            {"results": []},
            {"results": [{}, {}]},
            {"version": "1", "total_kv_cache": "100", "cache_keys": None},
            {"version": "1"},
            status(1, available=101),
        )
        for sample in invalid_samples:
            with self.subTest(sample=sample):
                calls = []
                with self.assertRaises(CachePublicationError):
                    run_cache_publication_pair(
                        lambda: calls.append("producer"),
                        lambda: calls.append("consumer"),
                        lambda timeout: sample,
                        0.1,
                    )
                self.assertEqual(calls, [])

    def test_producer_failure_prevents_consumer(self):
        calls = []

        def producer():
            calls.append("producer")
            raise CachePublicationError("producer failed")

        with self.assertRaisesRegex(CachePublicationError, "producer failed"):
            run_cache_publication_pair(
                producer,
                lambda: calls.append("consumer"),
                lambda timeout: status(),
                0.1,
            )
        self.assertEqual(calls, ["producer"])

    def test_poll_failure_aborts_without_retrying_consumer(self):
        samples = iter((status(), {"error": "backend unavailable"}))
        calls = []
        with self.assertRaisesRegex(CachePublicationError, "backend unavailable"):
            run_cache_publication_pair(
                lambda: calls.append("producer"),
                lambda: calls.append("consumer"),
                lambda timeout: next(samples),
                0.1,
            )
        self.assertEqual(calls, ["producer"])

    def test_stale_publication_timeout_never_sends_consumer(self):
        calls = []
        with self.assertRaisesRegex(CachePublicationTimeout, "deadline"):
            run_cache_publication_pair(
                lambda: calls.append("producer"),
                lambda: calls.append("consumer"),
                lambda timeout: status(),
                0.001,
            )
        self.assertEqual(calls, ["producer"])


class CachePublicationHttpTest(unittest.TestCase):
    def setUp(self):
        self.status_calls = []
        self.inference_calls = []
        self.http_order = []
        self.status_error = False
        self.slow_status = False
        self.release_http = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                owner.http_order.append(self.path)
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/rtp_llm/cache_status":
                    owner.status_calls.append(body)
                    if owner.slow_status:
                        owner.release_http.wait(2)
                    code = 503 if owner.status_error else 200
                    payload = (
                        status()
                        if len(owner.status_calls) == 1
                        else status(2, (10, 11))
                    )
                else:
                    owner.inference_calls.append(self.path)
                    code = 500 if self.path == "/consumer" else 200
                    payload = {"response": "unchanged"}
                encoded = json.dumps(payload).encode()
                try:
                    self.send_response(code)
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = "http://127.0.0.1:%d" % self.server.server_address[1]

    def tearDown(self):
        self.release_http.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def test_real_status_http_error_is_immediate_and_not_retried(self):
        self.status_error = True
        with self.assertRaisesRegex(CachePublicationError, "503"):
            fetch_cache_status(self.base_url + "/rtp_llm/cache_status", 1)
        self.assertEqual(len(self.status_calls), 1)
        self.assertEqual(self.status_calls[0]["need_cache_keys"], True)

    def test_complete_http_exchange_respects_deadline(self):
        self.slow_status = True
        started = time.monotonic()
        with self.assertRaises(CachePublicationError):
            fetch_cache_status(self.base_url + "/rtp_llm/cache_status", 0.08)
        # The peer waits 2s; it must not control the caller's total deadline.
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(len(self.status_calls), 1)

    def test_case_runner_uses_preproducer_snapshot_and_one_failed_consumer(self):
        # Load the actual runner/comparer classes without their GPU/runtime
        # imports. Their HTTP/retry and barrier control flow remains unchanged.
        root = pathlib.Path(__file__).resolve().parent

        class SmokeException(Exception):
            def __init__(self, error_status, message):
                self.error_status, self.message = error_status, message

        query_status = SimpleNamespace(
            OK="ok", VISIT_FAILED="visit_failed", VALID_FAILED="invalid", OTHERS="other"
        )
        namespace = {
            "__name__": "cache_publication_wiring_test",
            "logging": logging,
            "os": os,
            "time": time,
            "QueryStatus": query_status,
            "SmokeException": SmokeException,
            "Tracer": SimpleNamespace,
            "CachePublicationError": CachePublicationError,
            "fetch_cache_status": fetch_cache_status,
            "run_cache_publication_pair": run_cache_publication_pair,
            "save_response": lambda: False,
            "no_compare": lambda: False,
        }
        for filename, name in (
            ("base_comparer.py", "BaseComparer"),
            ("case_runner.py", "CaseRunner"),
        ):
            tree = ast.parse((root / filename).read_text())
            cls = next(
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == name
            )
            module = ast.Module(
                body=[
                    ast.ImportFrom(
                        module="__future__",
                        names=[ast.alias(name="annotations")],
                        level=0,
                    ),
                    cls,
                ],
                type_ignores=[],
            )
            exec(
                compile(ast.fix_missing_locations(module), filename, "exec"), namespace
            )

        class Comparer(namespace["BaseComparer"]):
            def format_query(self, value):
                return SimpleNamespace(model_dump=lambda **kwargs: value)

            def format_result(self, value):
                return value

            def curl_response_to_json(self, query, response):
                return json.loads(response)

            def compare_result(self, expected, actual):
                self.assertion(expected, actual)

            assertion = staticmethod(self.assertEqual)

            def _dump_actual_to_artifact(self, value):
                pass

        retry_arguments = []
        owner = self

        class Manager:
            port = owner.server.server_address[1]

            def visit(self, query, retries, endpoint, expected_status):
                retry_arguments.append(retries)
                # Exercise a real HTTP failure; the production comparer controls
                # how many transport attempts are permitted for this fixture.
                for _ in range(retries):
                    request = urllib.request.Request(
                        owner.base_url + endpoint,
                        data=json.dumps(query).encode(),
                        headers={"Content-Type": "application/json"},
                    )
                    try:
                        with urllib.request.urlopen(request, timeout=1) as response:
                            return True, response.read().decode()
                    except urllib.error.HTTPError:
                        pass
                return False, None

        class States:
            def __init__(self):
                self.ret = True
                self.query_status = []

        namespace.update(TaskStates=States, EmbeddingComparer=object())
        runner = namespace["CaseRunner"].__new__(namespace["CaseRunner"])
        runner.parallel_qr = 1
        runner.concurrency_test = False
        runner.batch_infer = False
        runner._get_comparer_cls = lambda qr, endpoint: Comparer
        runner._run_stability_repeat = lambda *args: None
        task = SimpleNamespace(
            query_result=[
                {
                    "endpoint": "/producer",
                    "query": {},
                    "result": {"response": "unchanged"},
                },
                {"endpoint": "/consumer", "query": {}, "result": {}},
            ],
            endpoint="/",
            taskinfo_rel_path="fixture.json",
            tokenizer_path=None,
            model_path="unused",
            model_type="unused",
            grammar_constraint_only=False,
            cache_publication_barrier=SimpleNamespace(timeout_s=1),
        )
        result = runner._curl_server_impl(Manager(), task)
        self.assertFalse(result.ret)
        self.assertEqual(
            [row[0] for row in result.query_status], ["ok", "visit_failed"]
        )
        self.assertEqual(retry_arguments, [1, 1])
        self.assertEqual(self.inference_calls, ["/producer", "/consumer"])
        self.assertEqual(len(self.status_calls), 2)
        self.assertTrue(all(item["need_cache_keys"] for item in self.status_calls))
        self.assertEqual(
            self.http_order,
            [
                "/rtp_llm/cache_status",
                "/producer",
                "/rtp_llm/cache_status",
                "/consumer",
            ],
        )
        # A normal comparer retains the existing environment/default retry count.
        retry_arguments.clear()
        Comparer(
            Manager(),
            "/producer",
            {"query": {}, "result": {"response": "unchanged"}},
            SimpleNamespace(),
            False,
        ).run()
        self.assertEqual(retry_arguments, [int(os.environ.get("VISIT_RETRY_TIME", 4))])


if __name__ == "__main__":
    unittest.main()
