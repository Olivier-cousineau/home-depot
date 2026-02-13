import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from playwright.sync_api import Browser, BrowserContext, Page, Playwright

DEFAULT_STORES_PATH = Path("data/home_depot_stores.json")
DEFAULT_PRODUCTS_PATH = Path("data/homedepot_products.json")
DEFAULT_OUTPUT_DIR = Path("public/homedepot")
DEFAULT_INDEX_PATH = Path("public/index/homedepot-deals.json")
DEFAULT_MAX_STORES = 5
DEFAULT_SLEEP = 0.5
REQUEST_TIMEOUT_MS = 45_000


@dataclass
class Store:
    storeId: str
    city: str
    province: str
    postalCode: str
    slug: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Store":
        store_id = str(data.get("storeId") or data.get("store_number") or "")
        city = str(data.get("city") or "")
        province = str(data.get("province") or "").upper()
        slug = str(data.get("slug") or _build_store_slug(store_id, city, province))
        return cls(
            storeId=store_id,
            city=city,
            province=province,
            postalCode=str(data.get("postalCode") or "").strip(),
            slug=slug,
        )


def _build_store_slug(store_id: str, city: str, province: str) -> str:
    def slugify(value: str) -> str:
        text = (value or "").strip().lower()
        text = re.sub(r"[^a-z0-9]+", "-", text)
        text = re.sub(r"-+", "-", text).strip("-")
        return text or "unknown"

    if not store_id:
        return f"{slugify(city)}-{slugify(province)}"
    return f"{store_id}-{slugify(city)}-{slugify(province)}"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Home Depot store-level offers for product URLs")
    parser.add_argument("--stores", type=Path, default=DEFAULT_STORES_PATH, help="Path to stores JSON")
    parser.add_argument(
        "--products",
        "--skus",
        dest="products",
        type=Path,
        default=DEFAULT_PRODUCTS_PATH,
        help="Path to products JSON",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for store JSON files")
    parser.add_argument("--index-output", type=Path, default=DEFAULT_INDEX_PATH, help="Aggregated index output file")
    parser.add_argument("--max-stores", type=int, default=DEFAULT_MAX_STORES, help="Maximum stores to process")
    parser.add_argument("--max-skus", type=int, default=0, help="Maximum SKUs to process per store (0 = no limit)")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Sleep between SKU checks")
    parser.add_argument("--retries", type=int, default=2, help="Retries for navigation/UI operations")
    parser.add_argument("--headful", action="store_true", help="Run browser in headed mode")
    return parser.parse_args()


def _extract_sku_from_url(url: str) -> str:
    match = re.search(r"/(\d+)(?:[/?#]|$)", url)
    return match.group(1) if match else ""


def _load_json_list(path: Path, label: str) -> List[Dict[str, Any]]:
    if not path.exists():
        suggestion = " As-tu commité data/homedepot_products.json ?" if label == "products" else ""
        raise FileNotFoundError(f"{label} file not found: {path}.{suggestion}")

    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, list):
        raise ValueError(f"{label} must be a JSON array: {path}")

    if label == "products":
        items: List[Any] = data
        if items and isinstance(items[0], str):
            items = [{"url": str(s).strip()} for s in items if str(s).strip()]

        normalized: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()
        for item in items:
            if isinstance(item, str):
                row = {"url": item.strip()}
            elif isinstance(item, dict):
                row = dict(item)
            else:
                continue

            url = str(row.get("url") or "").strip()
            if not url:
                continue

            sku_value = str(row.get("sku") or "").strip() or _extract_sku_from_url(url)
            row["url"] = url
            row["sku"] = sku_value
            unique_id = sku_value or url
            if unique_id in seen_ids:
                continue

            seen_ids.add(unique_id)
            normalized.append(row)

        return normalized

    return data


def _clean_price(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(r"\$\s?\d[\d,]*(?:\.\d{2})?", text)
    if match:
        return match.group(0).replace(" ", "")
    return None


def _extract_int(text: str) -> Optional[int]:
    if not text:
        return None
    match = re.search(r"(\d+)", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _first_non_empty(values: List[Optional[str]]) -> Optional[str]:
    for value in values:
        if value and value.strip():
            return value.strip()
    return None


def _safe_text(page: "Page", selectors: List[str]) -> Optional[str]:
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() and locator.is_visible(timeout=1200):
                txt = locator.inner_text(timeout=1500).strip()
                if txt:
                    return txt
        except Exception:
            continue
    return None


def _click_first(page: "Page", selectors: List[str], timeout_ms: int = 1800) -> bool:
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() and locator.is_visible(timeout=timeout_ms):
                locator.click(timeout=timeout_ms)
                return True
        except Exception:
            continue
    return False


def _set_store_context(page: "Page", store: Store) -> bool:
    opened = _click_first(
        page,
        [
            'button:has-text("My Store")',
            'button:has-text("Select Store")',
            'button:has-text("Choose Store")',
            'button:has-text("Pickup")',
            '[data-testid*="store"] button',
        ],
    )
    if not opened:
        return False

    search_value = store.postalCode or store.storeId or store.city
    if search_value:
        input_selectors = [
            'input[placeholder*="Postal"]',
            'input[placeholder*="postal"]',
            'input[placeholder*="store"]',
            'input[type="search"]',
            'input[name*="search"]',
        ]
        for selector in input_selectors:
            locator = page.locator(selector).first
            try:
                if locator.count() and locator.is_visible(timeout=1800):
                    locator.fill(str(search_value), timeout=1800)
                    locator.press("Enter")
                    time.sleep(0.4)
                    break
            except Exception:
                continue

    selected = _click_first(
        page,
        [
            f'button:has-text("{store.storeId}")',
            f'button:has-text("{store.city}")',
            f'button:has-text("{store.postalCode}")',
            'button:has-text("Set as My Store")',
            'button:has-text("Select")',
            'button:has-text("Choose")',
            '[data-testid*="set-store"]',
        ],
        timeout_ms=2500,
    )
    if not selected:
        return False

    time.sleep(1.0)
    return True


def _extract_store_offer(page: "Page", sku: Dict[str, Any]) -> Dict[str, Any]:
    online_regular = sku.get("price_regular")
    online_clearance = sku.get("price_clearance")

    price_now = _clean_price(
        _first_non_empty(
            [
                _safe_text(page, ['[data-testid*="price"]', 'span:has-text("$")', '.price']),
                _safe_text(page, ['[class*="price"]', 'div:has-text("$")']),
            ]
        )
        or ""
    )

    regular_price = _clean_price(
        _first_non_empty(
            [
                _safe_text(page, [r'text=/Regular\s*\$/', r'text=/Was\s*\$/', '[class*="regular"]']),
                str(online_regular) if online_regular is not None else None,
            ]
        )
        or ""
    )

    stock_text = _first_non_empty(
        [
            _safe_text(page, ['text=/In Stock/i', 'text=/Low Stock/i', r'text=/Only \d+ left/i']),
            _safe_text(page, ['[data-testid*="inventory"]', '[class*="stock"]']),
        ]
    )
    quantity = _extract_int(stock_text or "")

    return {
        "price": price_now,
        "price_regular": regular_price,
        "stock_status": stock_text,
        "quantity": quantity,
        "last_checked_at": datetime.now(timezone.utc).isoformat(),
        "status": "ok" if price_now or stock_text else "unknown",
        "price_clearance_online": online_clearance,
    }


def _check_one_sku(page: "Page", store: Store, sku: Dict[str, Any], retries: int) -> Dict[str, Any]:
    url = str(sku.get("url") or "").strip()
    if not url:
        sku_id = str(sku.get("sku") or "")
        print(f"[CHECK][SKIP] sku={sku_id} reason=missing_url", flush=True)
        return {
            "store_offer": {
                "price": None,
                "price_regular": None,
                "stock_status": None,
                "quantity": None,
                "last_checked_at": datetime.now(timezone.utc).isoformat(),
                "status": "missing_url",
            }
        }

    last_error: Optional[str] = None
    for attempt in range(1, retries + 2):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT_MS)
            page.wait_for_timeout(900)
            offer = _extract_store_offer(page, sku)
            return {"store_offer": offer}
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            last_error = "timeout" if "Timeout" in message else message
            print(
                f"[STORE-CHECK][RETRY] sku={sku.get('sku')} store={store.storeId} attempt={attempt} error: {message}",
                flush=True,
            )
        page.wait_for_timeout(600)

    return {
        "store_offer": {
            "price": None,
            "price_regular": None,
            "stock_status": None,
            "quantity": None,
            "last_checked_at": datetime.now(timezone.utc).isoformat(),
            "status": "unknown",
            "error": last_error,
        }
    }


def _base_item(store: Store, sku: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "retailer": "homedepot",
        "storeId": store.storeId,
        "store_slug": store.slug,
        "city": store.city,
        "province": store.province,
        "sku": str(sku.get("sku") or ""),
        "title": sku.get("title"),
        "url": sku.get("url"),
        "image": sku.get("image"),
        "price_regular": sku.get("price_regular"),
        "price_clearance_online": sku.get("price_clearance"),
        "status": "pending",
        "price": None,
        "checked_at": None,
        "store_offer": {},
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)


def _run_for_store(
    context: "BrowserContext",
    store: Store,
    skus: List[Dict[str, Any]],
    sleep_seconds: float,
    retries: int,
) -> List[Dict[str, Any]]:
    print(f"[STORE-CHECK] start store={store.storeId} city={store.city}", flush=True)
    page = context.new_page()
    offers: List[Dict[str, Any]] = []
    context_set = False
    try:
        page.goto("https://www.homedepot.ca/", wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT_MS)
        page.wait_for_timeout(800)
        context_set = _set_store_context(page, store)
        page.wait_for_timeout(1200)
    except Exception as exc:  # noqa: BLE001
        print(f"[STORE-CHECK][WARN] store={store.storeId} unable to set context once: {exc}", flush=True)

    for idx, sku in enumerate(skus, start=1):
        sku_id = sku.get("sku")
        print(f"[STORE-CHECK] store={store.storeId} sku={sku_id} ({idx}/{len(skus)})", flush=True)
        item = _base_item(store, sku)
        check_result = _check_one_sku(page, store, sku, retries)
        item.update(check_result)
        store_offer = item.get("store_offer") or {}
        if not context_set and store_offer.get("status") == "ok":
            store_offer["status"] = "unknown"
        item["status"] = store_offer.get("status")
        item["price"] = store_offer.get("price")
        item["checked_at"] = store_offer.get("last_checked_at")
        offers.append(item)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    page.close()
    return offers


def _create_browser(headful: bool) -> Tuple["Playwright", "Browser", "BrowserContext"]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - runtime dependency
        print(
            "[STORE-CHECK][CRITICAL] Playwright is required. Install with: pip install playwright && playwright install chromium",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=not headful)
    context = browser.new_context(locale="en-CA", timezone_id="America/Toronto")
    return playwright, browser, context


def main() -> None:
    args = _parse_args()
    if args.max_stores < 0:
        raise SystemExit("--max-stores must be >= 0")
    if args.max_skus < 0:
        raise SystemExit("--max-skus must be >= 0")

    stores_raw = _load_json_list(args.stores, "stores")
    skus_raw = _load_json_list(args.products, "products")
    stores = [Store.from_dict(row) for row in stores_raw[: args.max_stores]]
    skus = skus_raw[: args.max_skus] if args.max_skus > 0 else skus_raw

    print(f"[STORE-CHECK] stores={len(stores)} skus={len(skus)} sleep={args.sleep}s retries={args.retries}", flush=True)

    playwright, browser, context = _create_browser(args.headful)
    all_items: List[Dict[str, Any]] = []
    try:
        for store in stores:
            store_items = _run_for_store(context, store, skus, args.sleep, args.retries)
            output_path = args.output_dir / f"{store.slug}.json"
            _write_json(output_path, store_items)
            all_items.extend(store_items)
            print(f"[STORE-CHECK] wrote {output_path} ({len(store_items)} items)", flush=True)
    finally:
        context.close()
        browser.close()
        playwright.stop()

    index_payload = {
        "retailer": "homedepot",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stores_count": len(stores),
        "items_count": len(all_items),
        "items": all_items,
    }
    _write_json(args.index_output, index_payload)
    print(f"[STORE-CHECK] wrote {args.index_output}", flush=True)
    print("[STORE-CHECK] done", flush=True)


if __name__ == "__main__":
    main()
