import importlib
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

HD_BASE_URL = "https://www.homedepot.ca"
HD_SEARCH_URL = f"{HD_BASE_URL}/api/search/v1/search"
HD_CLEARANCE_WARMUP_URL = f"{HD_BASE_URL}/en/home/categories/all-collections/clearance.html"
HD_CLEARANCE_FILTER = "j2z-xmv-qs7-43j"
HD_COLLECTION = "clearance_deals_homedepot"
HD_MAX_PAGES = 40
HD_PAGE_SIZE = 40
HD_REQUEST_DELAY_SECONDS = 0.3

HD_HDRS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*",
    "Accept-Language": "en-CA,en;q=0.9",
    "Origin": HD_BASE_URL,
    "x-web-host": "www.homedepot.ca",
    "Referer": "https://www.homedepot.ca/search?q=*&filter=j2z",
    "hdca-consumer": "web",
    "x-requested-with": "XMLHttpRequest",
}


@dataclass
class HomeDepotClearanceConfig:
    max_pages: int = HD_MAX_PAGES
    page_size: int = HD_PAGE_SIZE
    request_delay_seconds: float = HD_REQUEST_DELAY_SECONDS
    min_price: float = 1.0
    min_discount_pct: int = 40
    timeout_warmup: int = 10
    timeout_search: int = 15
    impersonate: str = "chrome124"


def normalize_store_id(store_id: str | int) -> str:
    digits = re.sub(r"\D+", "", str(store_id or ""))
    if not digits:
        raise ValueError("store_id must contain at least one digit")
    return str(int(digits)).zfill(4)


def safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        for key in ("value", "amount", "price", "displayPrice", "formattedValue"):
            if key in value:
                parsed = safe_float(value.get(key))
                if parsed:
                    return parsed
        return 0.0
    match = re.search(r"-?\d+(?:[\s,]\d{3})*(?:[.,]\d+)?", str(value))
    if not match:
        return 0.0
    return float(match.group(0).replace(" ", "").replace(",", ""))


def extract_price(value: Any) -> float:
    return round(safe_float(value), 2)


def extract_products(payload: dict[str, Any]) -> list[dict[str, Any]]:
    products = payload.get("products", [])
    if isinstance(products, list):
        return [p for p in products if isinstance(p, dict)]
    if isinstance(products, dict):
        return [
            item
            for scheme in products.get("schemes", [])
            if isinstance(scheme, dict)
            for item in scheme.get("items", [])
            if isinstance(item, dict)
        ]
    return []


def _first_present(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _extract_product_id(product: dict[str, Any]) -> str:
    return str(_first_present(product, ("code", "productId", "itemId", "sku", "modelNumber", "id")) or "")


def _extract_category(product: dict[str, Any]) -> str:
    categories = product.get("categories") or []
    if isinstance(categories, list) and categories:
        last = categories[-1]
        if isinstance(last, dict) and last.get("name"):
            return str(last.get("name"))
        if isinstance(last, str):
            return last
    return "Home Depot"


def _extract_image(product: dict[str, Any]) -> str:
    image = product.get("imageUrl")
    if not image:
        images = product.get("images") or []
        if isinstance(images, list) and images:
            first = images[0]
            image = first.get("url") if isinstance(first, dict) else first
    return urljoin(HD_BASE_URL, str(image)) if image else ""


def _extract_stock(product: dict[str, Any]) -> int:
    inv = product.get("storeStock") or product.get("storeInventory") or product.get("stock") or {}
    if not isinstance(inv, dict):
        return 0
    stock = int(safe_float(inv.get("stockLevel") or inv.get("quantity") or 0))
    if stock == 0 and inv.get("stockLevelStatus") == "inStock":
        return 1
    return stock


def normalize_clearance_product(
    product: dict[str, Any],
    *,
    min_price: float = 1.0,
    min_discount_pct: int = 40,
) -> dict[str, Any] | None:
    if product.get("productType", "") == "Bundle":
        if product.get("bundleFulfillmentOptions", {}).get("pickUpMessage") == "bundle_pickup_not_available":
            return None
        current_price = extract_price(product.get("bundleTotalPurchasePrice"))
        original_price = extract_price(product.get("bundleTotalWasNow"))
    else:
        pricing = product.get("pricing") or {}
        current_price = extract_price(pricing.get("displayPrice"))
        original_price = extract_price(pricing.get("wasprice") or pricing.get("originalPrice"))
        savings = extract_price(pricing.get("savingsAmount"))
        if original_price == 0 and current_price > 0 and savings > 0:
            original_price = round(current_price + savings, 2)

    if current_price < min_price or original_price <= current_price:
        return None

    pct = int(round((original_price - current_price) / original_price * 100))
    if pct < min_discount_pct:
        return None

    relative_url = str(product.get("url") or "")
    deal = {
        "id": _extract_product_id(product),
        "title": str(product.get("name") or ""),
        "currentPrice": current_price,
        "originalPrice": original_price,
        "pct": pct,
        "stock": _extract_stock(product),
        "category": _extract_category(product),
        "url": urljoin(HD_BASE_URL, relative_url) if relative_url else "",
        "image": _extract_image(product),
    }
    return deal if deal["id"] or deal["title"] else None


class HomeDepotClearanceAPIClient:
    def __init__(self, session: Any | None = None, config: HomeDepotClearanceConfig | None = None):
        self.config = config or HomeDepotClearanceConfig()
        self.session = session or self._build_session()

    def _build_session(self) -> Any:
        curl_requests = importlib.import_module("curl_cffi.requests")
        return curl_requests.Session(impersonate=self.config.impersonate)

    def warm_up_store(self, store_id: str | int) -> str:
        store_clean = normalize_store_id(store_id)
        self.session.cookies.set("store", str(int(store_clean)), domain=".homedepot.ca")
        self.session.get(HD_CLEARANCE_WARMUP_URL, headers=HD_HDRS, timeout=self.config.timeout_warmup)
        return store_clean

    def _search_url(self, store_clean: str, page: int) -> str:
        return (
            f"{HD_SEARCH_URL}?q=*&store={store_clean}&page={page}"
            f"&filter={HD_CLEARANCE_FILTER}&pageSize={self.config.page_size}&lang=en"
        )

    def fetch_raw_products(self, store_id: str | int) -> list[dict[str, Any]]:
        store_clean = self.warm_up_store(store_id)
        raw_products: list[dict[str, Any]] = []

        for page in range(1, self.config.max_pages + 1):
            resp = self.session.get(self._search_url(store_clean, page), headers=HD_HDRS, timeout=self.config.timeout_search)
            if getattr(resp, "status_code", None) != 200:
                break
            batch = extract_products(resp.json())
            if not batch:
                break
            raw_products.extend(batch)
            time.sleep(self.config.request_delay_seconds)

        return raw_products

    def fetch_deals(self, store_id: str | int) -> list[dict[str, Any]]:
        return [
            deal
            for product in self.fetch_raw_products(store_id)
            if (
                deal := normalize_clearance_product(
                    product,
                    min_price=self.config.min_price,
                    min_discount_pct=self.config.min_discount_pct,
                )
            )
        ]
