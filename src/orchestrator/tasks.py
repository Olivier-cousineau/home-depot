import json
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.adapters.homedepot_ca_adapter import HomeDepotCAAdapter, Offer, offer_to_dict
from src.policy import SourcePolicy


@dataclass
class Task:
    retailer: str
    store_code: str
    skus: list[str]


class TokenBucket:
    def __init__(self, max_rps: float, burst: int):
        self.max_rps = max_rps
        self.burst = burst
        self.tokens = float(burst)
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                self.tokens = min(self.burst, self.tokens + elapsed * self.max_rps)
                self.last_refill = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            time.sleep(0.01)


class OfferCache:
    def __init__(self):
        self.values: dict[tuple[str, str, str], dict[str, Any]] = {}

    def _tier_to_ttl(self, tier: str) -> timedelta:
        return {"A": timedelta(minutes=60), "B": timedelta(hours=6), "C": timedelta(hours=24)}.get(
            tier, timedelta(hours=6)
        )

    def get(self, retailer: str, store: str, sku: str, tier: str) -> dict[str, Any] | None:
        key = (retailer, store, sku)
        payload = self.values.get(key)
        if not payload:
            return None
        if datetime.now(timezone.utc) > payload["expiry"]:
            self.values.pop(key, None)
            return None
        return payload["value"]

    def set(self, retailer: str, store: str, sku: str, value: dict[str, Any], tier: str):
        key = (retailer, store, sku)
        self.values[key] = {"value": value, "expiry": datetime.now(timezone.utc) + self._tier_to_ttl(tier)}


class DedupWindow:
    def __init__(self, minutes: int = 10):
        self.window = timedelta(minutes=minutes)
        self.seen: dict[tuple[str, str, str], tuple[float, str]] = {}

    def should_write(self, offer: Offer) -> bool:
        key = (offer.retailer, offer.store_code, offer.sku)
        signature = f"{offer.price}:{offer.availability}"
        now_ts = time.time()
        prev = self.seen.get(key)
        self.seen[key] = (now_ts, signature)
        if not prev:
            return True
        prev_ts, prev_sig = prev
        if prev_sig == signature and now_ts - prev_ts <= self.window.total_seconds():
            return False
        return True


def build_homedepot_tasks(stores: list[dict[str, Any]], skus: list[str], batch_size: int = 50) -> list[Task]:
    tasks: list[Task] = []
    for store in stores:
        store_code = str(store.get("store_number") or store.get("storeId"))
        for i in range(0, len(skus), batch_size):
            tasks.append(Task(retailer="homedepot_ca", store_code=store_code, skus=skus[i : i + batch_size]))
    return tasks


def _validate_offer(offer: Offer) -> tuple[bool, str | None]:
    if offer.price < 0:
        return False, "invalid_price"
    if offer.currency != "CAD":
        return False, "invalid_currency"
    if offer.promo is not None and offer.promo > offer.price:
        return False, "invalid_promo"
    if offer.availability not in {"in_stock", "out_of_stock", "limited", "unknown"}:
        return False, "invalid_availability"
    return True, None


def _retry_delay(attempt: int, retry_after: float | None = None) -> float:
    if retry_after is not None:
        return retry_after
    jitter = random.uniform(0, 0.3)
    return min(2**attempt + jitter, 30)


def run_homedepot_tasks(
    tasks: list[Task],
    source_policy: SourcePolicy,
    source_mode: str,
    output_path: str = "results/homedepot_offer_snapshots.json",
    max_rps: float = 2.0,
    burst: int = 4,
    max_retries: int = 3,
) -> dict[str, Any]:
    bucket = TokenBucket(max_rps=max_rps, burst=burst)
    cache = OfferCache()
    dedup = DedupWindow()
    adapter = HomeDepotCAAdapter(source_mode=source_mode)
    metrics = {"tasks_total": len(tasks), "tasks_skipped": 0, "offers_written": 0, "429_count": 0, "rps_limit": max_rps}

    offer_rows: list[dict[str, Any]] = []

    for task in tasks:
        if not source_policy.is_allowed(source_mode):
            metrics["tasks_skipped"] += 1
            print(
                f"[POLICY][blocked_by_policy] retailer={task.retailer} store={task.store_code} mode={source_mode} "
                f"message={source_policy.scrape_mode_message}"
            )
            continue

        bucket.acquire()
        for attempt in range(max_retries + 1):
            try:
                offers = adapter.fetch_offers(task.store_code, task.skus)
                for offer in offers:
                    valid, reason = _validate_offer(offer)
                    if not valid:
                        print(f"[VALIDATION][drop] sku={offer.sku} reason={reason}")
                        continue

                    cached = cache.get(offer.retailer, offer.store_code, offer.sku, tier="A")
                    if cached and cached.get("price") == offer.price and cached.get("availability") == offer.availability:
                        continue

                    if dedup.should_write(offer):
                        row = offer_to_dict(offer)
                        offer_rows.append(row)
                        metrics["offers_written"] += 1
                        cache.set(offer.retailer, offer.store_code, offer.sku, row, tier="A")
                break
            except TimeoutError:
                if attempt == max_retries:
                    break
                time.sleep(_retry_delay(attempt))
            except Exception as exc:
                code = getattr(exc, "status_code", None)
                if code == 429:
                    metrics["429_count"] += 1
                    retry_after = getattr(exc, "retry_after", None)
                    if attempt < max_retries:
                        time.sleep(_retry_delay(attempt, retry_after=retry_after))
                        continue
                if code in {400, 401, 403}:
                    break
                if code and code >= 500 and attempt < max_retries:
                    time.sleep(_retry_delay(attempt))
                    continue
                break

    Path("results").mkdir(exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "metrics": metrics, "offers": offer_rows}, f, indent=2)

    return metrics
