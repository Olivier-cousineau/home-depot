import unittest

from src.adapters.homedepot_clearance_api import (
    HD_CLEARANCE_FILTER,
    HomeDepotClearanceAPIClient,
    HomeDepotClearanceConfig,
    extract_products,
    normalize_clearance_product,
    normalize_store_id,
)


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class FakeCookies:
    def __init__(self):
        self.values = []

    def set(self, name, value, domain=None):
        self.values.append((name, value, domain))


class FakeSession:
    def __init__(self, responses):
        self.cookies = FakeCookies()
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


class HomeDepotClearanceAPITests(unittest.TestCase):
    def test_normalize_store_id_zero_pads_four_digits(self):
        self.assertEqual(normalize_store_id("71"), "0071")
        self.assertEqual(normalize_store_id("store-7166"), "7166")

    def test_extract_products_supports_direct_and_nested_schemas(self):
        self.assertEqual(extract_products({"products": [{"code": "1"}]}), [{"code": "1"}])
        nested = {"products": {"schemes": [{"items": [{"code": "2"}, "ignored"]}, {"items": [{"code": "3"}]}]}}
        self.assertEqual(extract_products(nested), [{"code": "2"}, {"code": "3"}])

    def test_normalize_standard_product_rebuilds_original_price_and_stock(self):
        deal = normalize_clearance_product(
            {
                "code": "1001",
                "name": "Drill",
                "url": "/product/drill/1001",
                "imageUrl": "/images/drill.jpg",
                "categories": [{"name": "Tools"}],
                "pricing": {"displayPrice": "$30.00", "savingsAmount": "$30.00"},
                "storeStock": {"stockLevel": 0, "stockLevelStatus": "inStock"},
            }
        )

        self.assertEqual(deal["id"], "1001")
        self.assertEqual(deal["currentPrice"], 30.0)
        self.assertEqual(deal["originalPrice"], 60.0)
        self.assertEqual(deal["pct"], 50)
        self.assertEqual(deal["stock"], 1)
        self.assertEqual(deal["category"], "Tools")
        self.assertEqual(deal["url"], "https://www.homedepot.ca/product/drill/1001")
        self.assertEqual(deal["image"], "https://www.homedepot.ca/images/drill.jpg")

    def test_normalize_product_rejects_low_discount_low_price_and_unavailable_bundle(self):
        self.assertIsNone(
            normalize_clearance_product({"code": "1", "name": "Cheap", "pricing": {"displayPrice": "$0.99", "wasprice": "$9.99"}})
        )
        self.assertIsNone(
            normalize_clearance_product({"code": "2", "name": "Small Sale", "pricing": {"displayPrice": "$80", "wasprice": "$100"}})
        )
        self.assertIsNone(
            normalize_clearance_product(
                {
                    "productType": "Bundle",
                    "code": "3",
                    "name": "Bundle",
                    "bundleFulfillmentOptions": {"pickUpMessage": "bundle_pickup_not_available"},
                    "bundleTotalPurchasePrice": "$40",
                    "bundleTotalWasNow": "$100",
                }
            )
        )

    def test_bundle_uses_bundle_prices_when_available(self):
        deal = normalize_clearance_product(
            {
                "productType": "Bundle",
                "code": "B1",
                "name": "Bundle",
                "bundleTotalPurchasePrice": "$40",
                "bundleTotalWasNow": "$100",
                "storeInventory": {"quantity": 2},
            }
        )
        self.assertEqual(deal["currentPrice"], 40.0)
        self.assertEqual(deal["originalPrice"], 100.0)
        self.assertEqual(deal["stock"], 2)

    def test_client_warms_cookie_and_paginates_clearance_api(self):
        session = FakeSession(
            [
                FakeResponse(200, {}),
                FakeResponse(200, {"products": [{"code": "1001", "name": "A", "pricing": {"displayPrice": "$40", "wasprice": "$100"}}]}),
                FakeResponse(200, {"products": []}),
            ]
        )
        client = HomeDepotClearanceAPIClient(
            session=session,
            config=HomeDepotClearanceConfig(max_pages=40, request_delay_seconds=0),
        )

        deals = client.fetch_deals("7166")

        self.assertEqual(session.cookies.values, [("store", "7166", ".homedepot.ca")])
        self.assertEqual(len(deals), 1)
        self.assertIn("clearance.html", session.calls[0][0])
        self.assertIn("page=1", session.calls[1][0])
        self.assertIn(f"filter={HD_CLEARANCE_FILTER}", session.calls[1][0])
        self.assertIn("pageSize=40", session.calls[1][0])
        self.assertIn("page=2", session.calls[2][0])


if __name__ == "__main__":
    unittest.main()
