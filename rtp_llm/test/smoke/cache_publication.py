"""Bounded cache-publication checks for a producer/consumer smoke pair.

Status exposes logical keys, not the completeness of every hybrid cache group.
The consumer's original reuse assertions must still check that contract.
"""

import json
import logging
import math
import queue
import re
import threading
import time
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, FrozenSet, Mapping, Tuple, TypeVar


class CachePublicationError(RuntimeError):
    pass


class CachePublicationTimeout(CachePublicationError):
    pass


def fetch_cache_status(url: str, timeout_s: float) -> Mapping[str, Any]:
    """Bound the complete HTTP exchange, including a slow response body.

    urllib's socket timeout alone is per operation. A daemon reader lets the
    caller enforce a single wall-clock budget even if the peer trickles bytes.
    A failed exchange aborts the smoke; it never starts another reader/retry.
    """
    deadline = time.monotonic() + timeout_s
    result_queue: queue.Queue = queue.Queue(maxsize=1)

    def read_response() -> None:
        try:
            request = urllib.request.Request(
                url,
                data=json.dumps(
                    {"latest_cache_version": -1, "need_cache_keys": True}
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            # This endpoint is on the smoke's own loopback server, never a proxy.
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CachePublicationTimeout("cache_status HTTP deadline expired")
            with opener.open(request, timeout=remaining) as response:
                payload = response.read(1024 * 1024 + 1)
                if len(payload) > 1024 * 1024:
                    raise CachePublicationError("cache_status response exceeds 1 MiB")
                status = json.loads(payload)
                if not isinstance(status, dict) or "error" in status:
                    raise CachePublicationError(
                        f"cache_status returned an error: {status}"
                    )
            result_queue.put((status, None))
        except Exception as error:
            result_queue.put((None, error))

    threading.Thread(target=read_response, daemon=True).start()
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise CachePublicationTimeout("cache_status HTTP deadline expired")
    try:
        status, error = result_queue.get(timeout=remaining)
    except queue.Empty as error:
        raise CachePublicationTimeout("cache_status HTTP deadline expired") from error
    if time.monotonic() >= deadline:
        raise CachePublicationTimeout("cache_status HTTP deadline expired")
    if error is not None:
        raise CachePublicationError(
            f"cache_status HTTP request failed: {error}"
        ) from error
    return status


def _integer(value: Any, field: str, minimum: int = 0) -> int:
    # protobuf JSON serializes int64/uint64 fields as decimal strings.
    if isinstance(value, str):
        try:
            value = int(value)
        except ValueError as error:
            raise CachePublicationError(
                f"invalid cache_status {field}: {value!r}"
            ) from error
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CachePublicationError(f"invalid cache_status {field}: {value!r}")
    return value


@dataclass(frozen=True)
class CacheStatusSnapshot:
    version: int
    keys: FrozenSet[int]
    available: int
    total: int
    address: str

    @classmethod
    def parse(cls, payload: Mapping[str, Any]) -> "CacheStatusSnapshot":
        if "error" in payload:
            raise CachePublicationError(f"cache_status returned an error: {payload}")
        if "results" in payload:
            results = payload["results"]
            if not isinstance(results, list) or len(results) != 1:
                raise CachePublicationError("cache publication requires one DP backend")
            payload = results[0]
        if not isinstance(payload, dict):
            raise CachePublicationError("cache_status backend must be an object")
        # CacheStatusPB.cache_keys is map<int64, bool>, not the cached_keys list
        # in the unrelated Python CacheStatus model. Proto JSON may omit defaults.
        cache_keys = payload.get("cache_keys", {})
        if not isinstance(cache_keys, dict):
            raise CachePublicationError("cache_status cache_keys must be an object")
        keys = set()
        for key, active in cache_keys.items():
            if not isinstance(key, str) or re.fullmatch(r"-?[0-9]+", key) is None:
                raise CachePublicationError(f"invalid cache_status cache key: {key!r}")
            # The server casts uint64 hashes to int64 before protobuf encoding.
            parsed_key = _integer(key, "cache_keys", minimum=-(1 << 63))
            if parsed_key > (1 << 63) - 1:
                raise CachePublicationError(f"cache key exceeds int64: {key!r}")
            if not isinstance(active, bool):
                raise CachePublicationError(
                    f"cache key state must be boolean: {active!r}"
                )
            if active:
                keys.add(parsed_key)
        snapshot = cls(
            # An empty SharedBlockCache starts at version -1, then advances on put.
            version=_integer(payload.get("version", 0), "version", minimum=-1),
            keys=frozenset(keys),
            available=_integer(
                payload.get("available_kv_cache", 0), "available_kv_cache"
            ),
            total=_integer(payload.get("total_kv_cache", 0), "total_kv_cache"),
            address=str(payload.get("address", "")),
        )
        if snapshot.total <= 0 or snapshot.available > snapshot.total:
            raise CachePublicationError(f"invalid cache_status capacity: {snapshot}")
        return snapshot


def wait_for_cache_publication(
    baseline: CacheStatusSnapshot,
    fetch_status: Callable[[float], Mapping[str, Any]],
    timeout_s: float,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    wait: Callable[[float], None] = time.sleep,
) -> CacheStatusSnapshot:
    """Wait for new logical keys and release of the producer's running blocks."""
    deadline = monotonic() + timeout_s
    last = baseline
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise CachePublicationTimeout(
                "cache publication deadline expired: "
                f"baseline={baseline}, last={last}"
            )
        # The HTTP budget can never exceed the remaining publication deadline.
        last = CacheStatusSnapshot.parse(fetch_status(remaining))
        if monotonic() >= deadline:
            raise CachePublicationTimeout("cache_status exceeded publication deadline")
        if last.address != baseline.address or last.total != baseline.total:
            raise CachePublicationError("cache_status backend/capacity changed")
        if (
            last.version > baseline.version
            and last.keys - baseline.keys
            and last.available == last.total
        ):
            return last
        # This only paces condition polling; elapsed time never proves readiness.
        wait(min(0.02, max(0.0, deadline - monotonic())))


_Result = TypeVar("_Result")


def run_cache_publication_pair(
    producer: Callable[[], _Result],
    consumer: Callable[[], _Result],
    fetch_status: Callable[[float], Mapping[str, Any]],
    timeout_s: float,
) -> Tuple[_Result, _Result]:
    """Run each inference exactly once, with an observed status barrier between."""
    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise CachePublicationError(
            "cache publication timeout must be positive and finite"
        )
    baseline = CacheStatusSnapshot.parse(fetch_status(timeout_s))
    logging.info("cache publication baseline before producer: %s", baseline)
    producer_result = producer()
    published = wait_for_cache_publication(baseline, fetch_status, timeout_s)
    logging.info(
        "cache publication observed before consumer: version=%d new_keys=%s available=%d/%d",
        published.version,
        sorted(published.keys - baseline.keys),
        published.available,
        published.total,
    )
    return producer_result, consumer()
