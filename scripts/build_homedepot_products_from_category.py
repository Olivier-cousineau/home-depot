import argparse
import json
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List
from urllib.parse import urljoin

if TYPE_CHECKING:
    from playwright.sync_api import Browser, BrowserContext, Page, Playwright

DEFAULT_OUTPUT_PATH = Path("data/homedepot_products.json")
REQUEST_TIMEOUT_MS = 45_000


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Home Depot products catalogue from a category URL")
    parser.add_argument("--category-url", required=True, help="HomeDepot category URL")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help="Output product catalogue path")
    parser.add_argument("--headful", action="store_true", help="Run browser in headed mode")
    return parser.parse_args()


def _extract_sku_from_url(url: str) -> str:
    match = re.search(r"/(\d+)(?:[/?#]|$)", url)
    return match.group(1) if match else ""


def _extract_products_from_dom(page: "Page") -> List[Dict[str, Any]]:
    payload = page.evaluate(
        """
        () => {
          const nodes = Array.from(document.querySelectorAll('a[href*="/product/"]'));
          const rows = [];
          for (const a of nodes) {
            const href = a.getAttribute('href') || '';
            const title = (a.getAttribute('title') || a.textContent || '').trim();
            const img = a.querySelector('img');
            const image = (img && (img.getAttribute('src') || img.getAttribute('data-src') || img.getAttribute('srcset'))) || '';
            if (!href) continue;
            rows.push({ href, title, image });
          }
          return rows;
        }
        """
    )

    items: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw in payload or []:
        href = str(raw.get("href") or "").strip()
        if not href:
            continue
        url = urljoin("https://www.homedepot.ca", href)
        sku = _extract_sku_from_url(url)
        unique_id = sku or url
        if unique_id in seen:
            continue
        seen.add(unique_id)
        items.append(
            {
                "sku": sku,
                "url": url,
                "title": str(raw.get("title") or "").strip() or None,
                "image": str(raw.get("image") or "").strip() or None,
            }
        )
    return items


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _create_browser(headful: bool) -> tuple["Playwright", "Browser", "BrowserContext"]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover
        print(
            "[CATALOG][CRITICAL] Playwright is required. Install with: pip install playwright && playwright install chromium",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=not headful)
    context = browser.new_context(locale="en-CA", timezone_id="America/Toronto")
    return playwright, browser, context


def main() -> None:
    args = _parse_args()

    playwright, browser, context = _create_browser(args.headful)
    try:
        page = context.new_page()
        page.goto(args.category_url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT_MS)
        page.wait_for_timeout(2_000)
        for _ in range(12):
            page.mouse.wheel(0, 2800)
            page.wait_for_timeout(650)

        products = _extract_products_from_dom(page)
    finally:
        context.close()
        browser.close()
        playwright.stop()

    _write_json(args.output, products)
    print(f"[CATALOG] wrote {args.output} ({len(products)} products)")


if __name__ == "__main__":
    main()
