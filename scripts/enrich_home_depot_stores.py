import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from home_depot_scraper import build_store_slug


STORE_URL_TEMPLATE = "https://www.homedepot.ca/store-details/{store_id}"
DEFAULT_STORES_PATH = Path("data/home_depot_stores.json")
FALLBACK_SHARDS_DIR = Path("shards")


@dataclass
class Store:
    storeId: str
    name: str
    city: str
    province: str
    postalCode: str
    slug: str

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "Store":
        store_id = str(data.get("storeId") or data.get("store_number"))
        return cls(
            storeId=store_id,
            name=data.get("name") or f"Home Depot {store_id}",
            city=data.get("city") or "",
            province=(data.get("province") or "").upper(),
            postalCode=data.get("postalCode") or "",
            slug=data.get("slug") or build_store_slug(store_id),
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "storeId": self.storeId,
            "name": self.name,
            "city": self.city,
            "province": self.province,
            "postalCode": self.postalCode,
            "slug": self.slug,
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

        new_slug = build_store_slug(
            self.storeId, city=self.city, province=self.province, fallback_slug=self.slug
        )
        if new_slug != self.slug:
            self.slug = new_slug
            updated = True

        return updated


def _configure_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1,
        raise_on_status=False,
    )
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
    response = session.get(url, timeout=(10, 60))
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


def main() -> None:
    stores = load_stores()
    session = _configure_session()
    enriched_count = 0
    total_needing_enrichment = sum(store.needs_enrichment() for store in stores)

    print(f"🔎 Stores à enrichir: {total_needing_enrichment}/{len(stores)}")

    for store in stores:
        if not store.needs_enrichment():
            store.apply_details({})
            continue

        try:
            details = fetch_store_details(session, store.storeId)
        except requests.RequestException as exc:
            print(f"[STORE {store.storeId}] ❌ Erreur réseau: {exc}")
            continue

        if not details:
            print(f"[STORE {store.storeId}] ⚠️ Aucune donnée trouvée")
            continue

        updated = store.apply_details(details)
        print(
            f"[STORE {store.storeId}] found city={store.city or '?'}; "
            f"province={store.province or '?'}; postalCode={store.postalCode or '?'}"
        )
        if updated:
            enriched_count += 1

    save_stores(stores)
    print(
        f"✅ Enrichment complete: enriched {enriched_count}/{len(stores)} stores."
    )


if __name__ == "__main__":
    main()
