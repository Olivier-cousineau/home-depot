import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

print("[ENRICH] script loaded OK", flush=True)


def build_store_slug(store_id: str, city: str, province: str) -> str:
    def slugify(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"[^a-z0-9]+", "-", s)
        s = re.sub(r"-+", "-").strip("-")
        return s or "unknown"

    return f"{store_id}-{slugify(city)}-{slugify(province)}"


STORE_URL_TEMPLATE = "https://www.homedepot.ca/store-details/{store_id}"
DEFAULT_STORES_PATH = Path("data/home_depot_stores.json")
FALLBACK_SHARDS_DIR = Path("shards")
DEFAULT_MAX_STORES = 25
REQUEST_TIMEOUT = 15


@dataclass
class Store:
    storeId: str
    name: str
    city: str
    province: str
    postalCode: str
    slug: str
    enrich_status: str = "ok"

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "Store":
        store_id = str(data.get("storeId") or data.get("store_number"))
        return cls(
            storeId=store_id,
            name=data.get("name") or f"Home Depot {store_id}",
            city=data.get("city") or "",
            province=(data.get("province") or "").upper(),
            postalCode=data.get("postalCode") or "",
            slug=data.get("slug")
            or build_store_slug(store_id, data.get("city") or "", data.get("province") or ""),
            enrich_status=data.get("enrich_status") or "ok",
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "storeId": self.storeId,
            "name": self.name,
            "city": self.city,
            "province": self.province,
            "postalCode": self.postalCode,
            "slug": self.slug,
            "enrich_status": self.enrich_status,
        }

    def needs_enrichment(self) -> bool:
        return not (self.city and self.province and self.postalCode)

    def apply_details(self, details: Dict[str, str]) -> bool:
        updated = False

        if details.get("name") and details["name"] != self.name:
            self.name = details["name"]
            updated = True

        for field in ["city", "province", "postalCode"]:
            new_value = details.get(field)
            if new_value and new_value != getattr(self, field):
                setattr(self, field, new_value)
                updated = True

        new_slug = build_store_slug(self.storeId, city=self.city, province=self.province)
        if new_slug != self.slug:
            self.slug = new_slug
            updated = True

        return updated


def _configure_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(total=0, connect=0, read=0, status=0, raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
            " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8",
        }
    )
    return session


def _load_fallback_stores() -> List[Dict[str, str]]:
    manifest_path = FALLBACK_SHARDS_DIR / "manifest.json"
    if not manifest_path.exists():
        return []

    try:
        with open(manifest_path, "r", encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
    except json.JSONDecodeError:
        return []

    stores: Dict[str, Dict[str, str]] = {}
    for shard in manifest.get("shards", []):
        shard_filename = shard.get("filename")
        if not shard_filename:
            continue
        shard_path = FALLBACK_SHARDS_DIR / shard_filename
        if not shard_path.exists():
            continue
        try:
            with open(shard_path, "r", encoding="utf-8") as shard_file:
                shard_data = json.load(shard_file)
        except json.JSONDecodeError:
            continue
        for store in shard_data.get("stores", []):
            store_id = str(store.get("storeId") or store.get("store_number"))
            if store_id and store_id not in stores:
                stores[store_id] = store
    return list(stores.values())


def load_stores() -> List[Store]:
    if DEFAULT_STORES_PATH.exists():
        with open(DEFAULT_STORES_PATH, "r", encoding="utf-8") as f:
            return [Store.from_dict(data) for data in json.load(f)]

    fallback_stores = _load_fallback_stores()
    if fallback_stores:
        return [Store.from_dict(data) for data in fallback_stores]

    raise FileNotFoundError(
        f"Aucun fichier de magasins trouvé à {DEFAULT_STORES_PATH} ou dans {FALLBACK_SHARDS_DIR}"
    )


def _parse_postal_code(text: str) -> Optional[str]:
    match = re.search(r"([A-Za-z]\d[A-Za-z])\s?-?\s?(\d[A-Za-z]\d)", text)
    if match:
        return f"{match.group(1)} {match.group(2)}".upper()
    return None


def _parse_city_province(text: str) -> Dict[str, str]:
    details: Dict[str, str] = {}
    city_prov_match = re.search(r"([A-Za-z\-\.\s']+),\s*([A-Za-z]{2})", text)
    if city_prov_match:
        details["city"] = city_prov_match.group(1).strip()
        details["province"] = city_prov_match.group(2).upper()
    return details


def _extract_from_ld_json(soup: BeautifulSoup) -> Dict[str, str]:
    details: Dict[str, str] = {}

    def _walk(node):
        if isinstance(node, dict):
            if node.get("@type") in {"Store", "LocalBusiness", "HardwareStore"}:
                address = node.get("address", {}) or {}
                if node.get("name"):
                    details.setdefault("name", node.get("name"))
                if address.get("addressLocality"):
                    details.setdefault("city", address.get("addressLocality"))
                if address.get("addressRegion"):
                    details.setdefault("province", address.get("addressRegion").upper())
                if address.get("postalCode"):
                    details.setdefault("postalCode", address.get("postalCode"))
            for value in node.values():
                _walk(value)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    for script_tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(script_tag.string or script_tag.get_text())
        except json.JSONDecodeError:
            continue
        _walk(data)

    return details


def _extract_from_html(soup: BeautifulSoup) -> Dict[str, str]:
    details: Dict[str, str] = {}
    address_nodes = [
        soup.find(attrs={"itemprop": "address"}),
        soup.find("address"),
    ]

    for node in address_nodes:
        if not node:
            continue
        text = node.get_text(" ", strip=True)
        if not text:
            continue
        postal = _parse_postal_code(text)
        if postal:
            details.setdefault("postalCode", postal)
        city_prov = _parse_city_province(text)
        details.update({k: v for k, v in city_prov.items() if v})

    if not details.get("city"):
        city_node = soup.find(attrs={"itemprop": "addressLocality"})
        if city_node:
            details["city"] = city_node.get_text(strip=True)

    if not details.get("province"):
        province_node = soup.find(attrs={"itemprop": "addressRegion"})
        if province_node:
            details["province"] = province_node.get_text(strip=True).upper()

    if not details.get("postalCode"):
        postal_node = soup.find(attrs={"itemprop": "postalCode"})
        if postal_node:
            details["postalCode"] = postal_node.get_text(strip=True).upper()

    if not details.get("name"):
        title = soup.find("h1")
        if title:
            details["name"] = title.get_text(strip=True)

    page_text = soup.get_text(" ", strip=True)
    if page_text:
        postal = details.get("postalCode") or _parse_postal_code(page_text)
        if postal:
            details.setdefault("postalCode", postal)
        if "city" not in details or "province" not in details:
            city_prov = _parse_city_province(page_text)
            for key, value in city_prov.items():
                if value and key not in details:
                    details[key] = value

    return details


def fetch_store_details(session: requests.Session, store_id: str) -> Dict[str, str]:
    url = STORE_URL_TEMPLATE.format(store_id=store_id)
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    details = _extract_from_ld_json(soup)
    html_details = _extract_from_html(soup)
    for key, value in html_details.items():
        details.setdefault(key, value)

    return details


def save_stores(stores: List[Store]) -> None:
    DEFAULT_STORES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DEFAULT_STORES_PATH, "w", encoding="utf-8") as f:
        json.dump([store.to_dict() for store in stores], f, ensure_ascii=False, indent=2)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich Home Depot stores data")
    parser.add_argument(
        "--max-stores",
        type=int,
        help="Maximum number of stores to process (overrides ENRICH_MAX_STORES)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without writing changes (overrides DRY_RUN)",
    )
    return parser.parse_args()


def _get_max_stores(arg_value: Optional[int]) -> int:
    if arg_value is not None:
        return max(arg_value, 0)
    return max(int(os.getenv("ENRICH_MAX_STORES", str(DEFAULT_MAX_STORES))), 0)


def _is_dry_run(arg_dry_run: bool) -> bool:
    if arg_dry_run:
        return True
    return os.getenv("DRY_RUN", "0") == "1"


def main() -> None:
    args = _parse_args()
    max_stores = _get_max_stores(args.max_stores)
    dry_run = _is_dry_run(args.dry_run)
    skip_timeouts = os.getenv("ENRICH_SKIP_TIMEOUTS", "0") == "1"

    try:
        stores = load_stores()
    except FileNotFoundError as exc:
        print(f"[ENRICH][CRITICAL] {exc}", file=sys.stderr)
        raise SystemExit(1)

    session = _configure_session()
    enriched_count = 0
    success_count = 0
    timeout_count = 0
    error_count = 0
    target_stores: List[Store] = []
    for store in stores:
        if store.needs_enrichment():
            target_stores.append(store)
        else:
            store.apply_details({})
            store.enrich_status = store.enrich_status or "ok"

    total_targets = len(target_stores)
    limit = min(total_targets, max_stores) if max_stores else total_targets

    print(f"🔎 Stores à enrichir: {total_targets}/{len(stores)} (processing {limit})")

    for idx, store in enumerate(target_stores[:limit], start=1):
        start_time = time.monotonic()
        print(
            f"[ENRICH] Processing store {idx}/{limit} - {store.storeId} - {store.name}",
            flush=True,
        )
        attempts_allowed = 1 if skip_timeouts else 2
        attempts_made = 0
        while attempts_made < attempts_allowed:
            attempts_made += 1
            try:
                details = fetch_store_details(session, store.storeId)
                if not details:
                    print(f"[ENRICH][ERROR] store {store.storeId} no data returned")
                    store.enrich_status = "error"
                    error_count += 1
                    break

                updated = store.apply_details(details)
                store.enrich_status = "ok"
                if updated:
                    enriched_count += 1
                duration = time.monotonic() - start_time
                success_count += 1
                print(
                    f"[ENRICH] Done store {store.storeId} in {duration:.2f}s",
                    flush=True,
                )
                break
            except requests.exceptions.Timeout as exc:
                if attempts_made >= attempts_allowed:
                    duration = time.monotonic() - start_time
                    timeout_count += 1
                    store.enrich_status = "timeout"
                    print(
                        f"[ENRICH][SKIP] store {store.storeId} after {attempts_made} retries (timeout) ({duration:.2f}s)",
                        flush=True,
                    )
                    break
                print(
                    f"[ENRICH][RETRY] store {store.storeId} timeout on attempt {attempts_made}/{attempts_allowed}",
                    flush=True,
                )
                continue
            except requests.exceptions.HTTPError as exc:
                if attempts_made >= attempts_allowed:
                    duration = time.monotonic() - start_time
                    error_count += 1
                    store.enrich_status = "error"
                    status_code = exc.response.status_code if exc.response else "unknown"
                    print(
                        f"[ENRICH][SKIP] store {store.storeId} after {attempts_made} retries (http {status_code}) ({duration:.2f}s)",
                        flush=True,
                    )
                    break
                print(
                    f"[ENRICH][RETRY] store {store.storeId} HTTP error on attempt {attempts_made}/{attempts_allowed}",
                    flush=True,
                )
                continue
            except Exception as exc:  # noqa: BLE001
                duration = time.monotonic() - start_time
                error_count += 1
                store.enrich_status = "error"
                print(f"[ENRICH][ERROR] store {store.storeId} {exc} ({duration:.2f}s)")
                break

    if dry_run:
        print("[ENRICH] DRY_RUN enabled - no changes will be written")
    else:
        save_stores(stores)
        print(f"[ENRICH] Saved updates to {DEFAULT_STORES_PATH}")

    print(f"[ENRICH] done: {success_count} success / {timeout_count} timeout / {error_count} error", flush=True)
    print(
        f"✅ Enrichment complete: enriched {enriched_count}/{limit} processed stores.",
        flush=True,
    )


if __name__ == "__main__":
    main()
