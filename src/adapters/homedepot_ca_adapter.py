import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class Offer:
    retailer: str
    store_code: str
    sku: str
    observed_at: str
    price: float
    currency: str
    promo: float | None
    availability: str
    quantity_hint: str | None
    source_type: str
    source_ref: str
    raw_minimal: dict[str, Any]


class HomeDepotCAAdapter:
    """Adapter for authorized sources only (partner API/feed)."""

    def __init__(self, source_mode: str = "partner_api"):
        self.source_mode = source_mode
        self.partner_api_key = os.getenv("HOMEDEPOT_PARTNER_API_KEY")
        self.partner_feed_path = os.getenv("HOMEDEPOT_PARTNER_FEED", "")

    def _iso_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _build_raw_minimal(self, payload: dict[str, Any]) -> dict[str, Any]:
        essentials = {
            "sku": str(payload.get("sku", "")),
            "store_code": str(payload.get("store_code", "")),
            "price": payload.get("price"),
            "promo": payload.get("promo"),
            "availability": payload.get("availability", "unknown"),
        }
        digest = hashlib.sha256(json.dumps(essentials, sort_keys=True).encode("utf-8")).hexdigest()
        return {"payload_hash": digest, "essentials": essentials}

    def _load_partner_feed(self) -> list[dict[str, Any]]:
        if not self.partner_feed_path or not os.path.exists(self.partner_feed_path):
            return []
        with open(self.partner_feed_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []

    def fetch_offers(self, store_code: str, product_ids: list[str]) -> list[Offer]:
        if self.source_mode != "partner_api":
            return []

        # Requires an authorized source (key or curated feed).
        if not self.partner_api_key and not self.partner_feed_path:
            return []

        feed_rows = self._load_partner_feed()
        if feed_rows:
            rows = [
                r
                for r in feed_rows
                if str(r.get("store_code")) == str(store_code) and str(r.get("sku")) in {str(s) for s in product_ids}
            ]
        else:
            # Placeholder: in real integration, call partner API with key.
            rows = []

        offers: list[Offer] = []
        for row in rows:
            raw_minimal = self._build_raw_minimal(row)
            offers.append(
                Offer(
                    retailer="homedepot_ca",
                    store_code=str(store_code),
                    sku=str(row.get("sku")),
                    observed_at=self._iso_now(),
                    price=float(row.get("price", 0.0)),
                    currency=str(row.get("currency", "CAD")),
                    promo=float(row["promo"]) if row.get("promo") is not None else None,
                    availability=str(row.get("availability", "unknown")),
                    quantity_hint=row.get("quantity_hint"),
                    source_type="partner_api",
                    source_ref=str(row.get("source_ref", "partner_feed")),
                    raw_minimal=raw_minimal,
                )
            )
        return offers


def offer_to_dict(offer: Offer) -> dict[str, Any]:
    return asdict(offer)
