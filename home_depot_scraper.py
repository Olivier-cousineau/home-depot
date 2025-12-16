import argparse
import csv
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

VERBOSE = os.getenv("VERBOSE", "0") == "1"


def slugify(text):
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def build_store_slug(store_id, city=None, province=None, fallback_slug=None):
    slug_parts = [str(store_id).strip()]
    if city:
        slug_parts.append(slugify(city))
    if province:
        slug_parts.append(slugify(province))

    computed_slug = "-".join([part for part in slug_parts if part])
    if computed_slug.strip("-"):
        return computed_slug

    if fallback_slug:
        return fallback_slug

    return f"store-{store_id}"


def vprint(*args):
    if VERBOSE:
        print(*args, flush=True)


def safe_get(session, url, retries=3, backoff_factor=1, **kwargs):
    """Wrapper autour de session.get avec backoff et logs verboses."""

    for attempt in range(retries):
        try:
            return session.get(url, **kwargs)
        except requests.exceptions.RequestException as exc:
            if attempt == retries - 1:
                vprint(f"safe_get: échec pour {url} après {retries} tentatives ({exc})")
                raise

            wait_time = backoff_factor * (2 ** attempt)
            vprint(f"safe_get: tentative {attempt + 1}/{retries} échouée ({exc}), nouvelle tentative dans {wait_time}s")
            time.sleep(wait_time)


class StoreDeadlineExceeded(Exception):
    """Raised when a store processing deadline is reached."""


class HomeDepotScraper:
    def __init__(self):
        self.base_url = "https://www.homedepot.ca"
        self.api_base = "https://www.homedepot.ca/api"
        self.ci_mode = os.getenv("CI", "").lower() in {"1", "true", "yes"}
        self.timeout = (10, 15) if self.ci_mode else (10, 90)
        self.max_minutes_per_store = int(os.getenv("MAX_MINUTES_PER_STORE", "25"))
        self.safe_mode = os.getenv("SAFE_MODE", "0") == "1"
        self.max_concurrency = int(os.getenv("MAX_CONCURRENCY", "4"))
        if self.safe_mode:
            self.max_concurrency = min(self.max_concurrency, 2)
        self.verbose = os.getenv("VERBOSE", "0") == "1"
        self.summary = {"success": 0, "skipped_ci": 0, "errors": 0}

        # Rotation des User-Agents pour éviter la détection
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

        self.session = requests.Session()
        self.configure_session()
        self.products = []
        self.stores = []
        self.update_headers()

    def _slugify(self, text):
        return slugify(text)

    def _parse_store_slug(self, store_url, fallback_name=None):
        parsed = urlparse(store_url)
        parts = [p for p in parsed.path.split("/") if p]

        if len(parts) >= 3 and parts[-2] != "store-details":
            return parts[-2]

        if fallback_name:
            return self._slugify(fallback_name)

        return None

    def configure_session(self):
        retry_kwargs = dict(
            total=8,
            connect=8,
            read=8,
            status=8,
            backoff_factor=1,
        )

        if self.ci_mode:
            retry_kwargs.update({"total": 1, "connect": 1, "read": 1, "status": 1, "backoff_factor": 0})

        retry_strategy = Retry(
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            raise_on_status=False,
            **retry_kwargs,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def update_headers(self):
        """Met à jour les headers avec un User-Agent aléatoire"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-CA,en;q=0.9,fr-CA;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.homedepot.ca/',
            'Origin': 'https://www.homedepot.ca',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })

    def smart_delay(self, min_delay=2, max_delay=5):
        """Délai aléatoire pour simuler un comportement humain"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def save_debug_proof(self, url, exception, retry_count, store_id=None, step=None):
        if not os.path.exists("debug"):
            os.makedirs("debug")

        filename = f"debug/{store_id or 'unknown'}_{step or 'request'}.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"URL: {url}\n")
            f.write(f"Exception: {exception}\n")
            f.write(f"Retry count: {retry_count}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
        print(f"🧾 Preuve de debug enregistrée: {filename}")

    def write_store_error(self, store_id, url, exception):
        if not os.path.exists("debug"):
            os.makedirs("debug")

        filename = f"debug/store_{store_id or 'unknown'}_error.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"URL: {url}\n")
            f.write(f"Exception: {exception}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
        print(f"🧾 Preuve de debug enregistrée: {filename}")

    def _enforce_deadline(self, deadline, store_id=None):
        if deadline and time.time() > deadline:
            raise StoreDeadlineExceeded(f"Store {store_id} deadline reached")

    def make_request(self, url, max_retries=8, use_json=False, store_id=None, step=None, deadline=None):
        """Effectue une requête avec retry et rotation de User-Agent"""
        effective_retries = 1 if self.ci_mode else max_retries

        for attempt in range(effective_retries):
            self._enforce_deadline(deadline, store_id)
            try:
                if attempt > 0:
                    self.update_headers()
                    wait_time = min(2 ** attempt + random.uniform(1, 3), 30)
                    print(f"⏳ Attente de {wait_time:.1f}s avant nouvelle tentative...")
                    time.sleep(wait_time)

                print(f"🔍 Requête: {url[:80]}... (Tentative {attempt + 1}/{max_retries})")
                start_time = time.monotonic()
                response = safe_get(self.session, url, timeout=self.timeout, headers=self.session.headers)
                duration = time.monotonic() - start_time
                print(f"🌐 URL: {url} | Status: {response.status_code} | Temps: {duration:.2f}s")

                if 'captcha' in response.text.lower() or response.status_code == 403:
                    print("⚠️  CAPTCHA détecté ou accès refusé - Changement de stratégie...")
                    self.smart_delay(10, 20)
                    continue

                response.raise_for_status()
                self.smart_delay()

                if use_json:
                    return response.json()
                return response

            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response else "N/A"
                print(f"❌ Erreur HTTP {status}: {e}")
                if status in [403, 429]:
                    self.save_debug_proof(url, e, attempt + 1, store_id, step)
                    self.write_store_error(store_id, url, e)
                if e.response.status_code == 429:
                    print("⚠️  Rate limit atteint - Pause prolongée...")
                    time.sleep(60)
                elif e.response.status_code in [403, 503]:
                    print(f"⚠️  Erreur {e.response.status_code} - Protection anti-bot...")
                    self.smart_delay(20, 40)
                else:
                    print(f"❌ Erreur HTTP {e.response.status_code}: {e}")
            except requests.exceptions.RequestException as e:
                print(f"❌ Erreur de connexion: {e}")
                if isinstance(e, requests.exceptions.Timeout):
                    self.save_debug_proof(url, e, attempt + 1, store_id, step)
                    if self.ci_mode and attempt == 0 and step in {"verify_store", "enrich_store"}:
                        print(f"[ENRICH][SKIP] store {store_id} - CI blocked by HomeDepot")
                        return None
                self.write_store_error(store_id, url, e)

            if attempt == effective_retries - 1:
                print(f"❌ Échec après {effective_retries} tentatives")
                self.save_debug_proof(url, Exception("Max retries exceeded"), attempt + 1, store_id, step)
                self.write_store_error(store_id, url, Exception("Max retries exceeded"))
                return None

        return None

    def get_all_stores(self, stores_file="data/home_depot_stores.json"):
        """Charge la liste des magasins à partir d'un fichier local versionné."""
        print("\n" + "=" * 70)
        print("📍 CHARGEMENT DES MAGASINS HOME DEPOT CANADA (SOURCE LOCALE)")
        print("=" * 70)

        try:
            with open(stores_file, "r", encoding="utf-8") as f:
                local_stores = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Le fichier {stores_file} est introuvable. Assurez-vous qu'il est versionné."
            )

        enriched_stores = []
        for store in local_stores:
            store_id = store.get("storeId") or store.get("store_number")
            store_details = {
                "storeId": store_id,
                "store_number": store_id,
                "name": store.get("name"),
                "city": store.get("city"),
                "province": store.get("province"),
                "postalCode": store.get("postalCode"),
                "slug": build_store_slug(
                    store_id,
                    city=store.get("city"),
                    province=store.get("province"),
                    fallback_slug=store.get("slug"),
                ),
                "url": f"{self.base_url}/store-details/{store_id}",
            }

            enriched_stores.append(store_details)

        self.stores = enriched_stores
        print(
            f"✅ {len(self.stores)} magasins chargés depuis {stores_file} (aucun appel réseau)"
        )
        return self.stores

    def _extract_address_details(self, text):
        details = {}
        if not text:
            return details

        postal_match = re.search(r"([A-Za-z]\d[A-Za-z])\s?-?\s?(\d[A-Za-z]\d)", text)
        if postal_match:
            details['postalCode'] = f"{postal_match.group(1)} {postal_match.group(2)}"

        city_prov_match = re.search(r"([A-Za-z\-\.\s]+),\s*([A-Za-z]{2})", text)
        if city_prov_match:
            details['city'] = city_prov_match.group(1).strip()
            details['province'] = city_prov_match.group(2).upper()

        return details

    def _enrich_store(self, store):
        store_id = store.get('store_number') or store.get('storeId')
        store_name = store.get('name')
        store_url = store.get('url')

        store_details = {
            'storeId': store_id,
            'store_number': store_id,
            'url': store_url,
            'name': store_name,
        }

        slug = self._parse_store_slug(store_url, fallback_name=store_name)
        if slug:
            store_details['slug'] = slug

        response = self.make_request(store_url, store_id=store_id, step="enrich_store", max_retries=4)
        if response and response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            name_elem = soup.find(['h1', 'h2'], class_=lambda x: x and 'store' in x.lower() if x else False)
            if name_elem:
                store_details['name'] = name_elem.get_text(strip=True)

            address_elem = soup.find('address')
            address_text = " ".join(address_elem.stripped_strings) if address_elem else None

            if not address_text:
                address_candidates = soup.find_all(string=re.compile(r"[A-Za-z]\d[A-Za-z]"))
                if address_candidates:
                    address_text = " ".join([s.strip() for s in address_candidates if s])

            address_info = self._extract_address_details(address_text or "")
            store_details.update(address_info)

            if 'slug' not in store_details:
                heading_text = store_details.get('name') or store_name or ""
                generated_slug = self._slugify(heading_text)
                if generated_slug:
                    store_details['slug'] = generated_slug

        return store_details

    def verify_store(self, store, deadline=None):
        """Vérifie si un magasin existe et récupère ses détails"""
        if self.ci_mode:
            store['verified'] = False
            print(f"[ENRICH][SKIP] store {store.get('store_number', 'Unknown')} - CI blocked by HomeDepot")
            return False
        self._enforce_deadline(deadline, store.get('store_number'))
        response = self.make_request(store['url'], store_id=store.get('store_number'), step="verify_store", deadline=deadline)
        if response and response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            store_name = soup.find(['h1', 'h2'], class_=lambda x: x and 'store' in x.lower() if x else False)
            if store_name:
                store['name'] = store_name.get_text(strip=True)
                store['verified'] = True
                return True
        store['verified'] = False
        return False

    def scrape_clearance_for_store(self, store, deadline=None):
        """Scrape les produits en liquidation pour un magasin spécifique"""
        print(f"\n🏪 Magasin: {store.get('name', store.get('store_number'))}")

        if self.ci_mode:
            print(f"[ENRICH][SKIP] store {store.get('store_number', 'Unknown')} - CI blocked by HomeDepot")
            return []

        clearance_urls = [
            f"{self.base_url}/en/search?q=clearance&storeId={store['store_number']}",
            f"{self.base_url}/fr/recherche?q=liquidation&storeId={store['store_number']}",
            f"{self.base_url}/en/deals/clearance?storeId={store['store_number']}",
        ]

        store_products = []

        for url in clearance_urls:
            self._enforce_deadline(deadline, store.get('store_number'))
            response = self.make_request(url, store_id=store.get('store_number'), step="clearance", deadline=deadline)
            if not response:
                continue

            soup = BeautifulSoup(response.content, 'html.parser')
            products = soup.find_all(['div', 'article'], class_=lambda x: x and ('product' in x.lower() or 'pod' in x.lower()) if x else False)

            for product_elem in products:
                self._enforce_deadline(deadline, store.get('store_number'))
                product_info = self.extract_product_info(product_elem, store)
                if product_info:
                    store_products.append(product_info)

            if products:
                break

        print(f"   ✓ {len(store_products)} produits en liquidation trouvés")
        return store_products

    def extract_product_info(self, product_elem, store):
        """Extrait les informations d'un produit"""
        try:
            product_data = {
                'store_number': store.get('store_number'),
                'store_name': store.get('name', 'Unknown')
            }

            name_elem = product_elem.find(['h3', 'h2', 'span', 'a'], class_=lambda x: x and ('title' in x.lower() or 'name' in x.lower()) if x else False)
            if not name_elem:
                name_elem = product_elem.find('a', class_=lambda x: x and 'product' in x.lower() if x else False)
            if name_elem:
                product_data['name'] = name_elem.get_text(strip=True)

            sku_elem = product_elem.find(['span', 'div'], string=re.compile(r'(SKU|Model):', re.I))
            if sku_elem:
                sku_text = sku_elem.get_text(strip=True)
                sku_match = re.search(r'(?:SKU|Model):\s*(\S+)', sku_text, re.I)
                if sku_match:
                    product_data['sku'] = sku_match.group(1)

            price_elem = product_elem.find(['span', 'div'], class_=lambda x: x and 'price' in x.lower() if x else False)
            if price_elem:
                product_data['price'] = price_elem.get_text(strip=True)

            was_price_elem = product_elem.find(['span', 'div'], class_=lambda x: x and ('was' in x.lower() or 'original' in x.lower()) if x else False)
            if was_price_elem:
                product_data['original_price'] = was_price_elem.get_text(strip=True)

            save_elem = product_elem.find(['span', 'div'], class_=lambda x: x and 'save' in x.lower() if x else False)
            if save_elem:
                product_data['savings'] = save_elem.get_text(strip=True)

            link_elem = product_elem.find('a', href=True)
            if link_elem:
                product_data['url'] = urljoin(self.base_url, link_elem['href'])

            img_elem = product_elem.find('img')
            if img_elem:
                product_data['image'] = img_elem.get('src', img_elem.get('data-src', ''))

            badge_elem = product_elem.find(['span', 'div'], class_=lambda x: x and ('clearance' in x.lower() or 'liquidation' in x.lower()) if x else False)
            if badge_elem:
                product_data['clearance_badge'] = badge_elem.get_text(strip=True)

            stock_elem = product_elem.find(['span', 'div'], string=re.compile(r'(in stock|en stock|available)', re.I))
            if stock_elem:
                product_data['availability'] = stock_elem.get_text(strip=True)

            product_data['scraped_at'] = datetime.now().isoformat()

            return product_data if product_data.get('name') else None

        except Exception as e:
            print(f"⚠️  Erreur extraction produit: {e}")
            return None

    def scrape_shard(self, shard_stores):
        """Scrape un shard spécifique de magasins"""
        verified_stores = 0
        for i, store in enumerate(shard_stores, 1):
            print(f"\n📊 Progression du shard: {i}/{len(shard_stores)}")

            if i % 3 == 0 or not store.get('verified'):
                if self.verify_store(store):
                    verified_stores += 1

            store_products = self.scrape_clearance_for_store(store)
            self.products.extend(store_products)

            if i % 3 == 0:
                print("⏸️  Pause de sécurité...")
                self.smart_delay(10, 15)

        return verified_stores

    def _create_store_worker(self):
        worker = HomeDepotScraper()
        worker.max_minutes_per_store = self.max_minutes_per_store
        worker.safe_mode = self.safe_mode
        worker.max_concurrency = self.max_concurrency
        worker.verbose = self.verbose
        worker.ci_mode = self.ci_mode
        worker.summary = self.summary
        worker.timeout = self.timeout
        return worker

    def _format_store_label(self, store):
        store_id = store.get('store_number') or store.get('storeId') or 'Unknown'
        location_parts = [store.get('city'), store.get('province')]
        location = ", ".join([p for p in location_parts if p])
        display = location or store.get('name') or f"Store {store_id}"
        return f"[STORE {store_id}] {display}"

    def process_store(self, store):
        store_copy = dict(store)
        store_id = store_copy.get('store_number', 'Unknown')
        store_products = []
        verified = False
        skipped_ci = 0
        success = 0
        errors = 0
        store_start = time.time()
        deadline = store_start + self.max_minutes_per_store * 60
        worker = self._create_store_worker()

        print(self._format_store_label(store_copy))

        try:
            if worker.ci_mode:
                skipped_ci = 1
                store_copy['verified'] = False
                print(f"[ENRICH][SKIP] store {store_id} - CI blocked by HomeDepot")
            else:
                verified = worker.verify_store(store_copy, deadline=deadline)
                store_copy['verified'] = verified
                store_products = worker.scrape_clearance_for_store(store_copy, deadline=deadline)
                success = 1
        except StoreDeadlineExceeded:
            print(f"[STORE {store_id}] ⏱️ Max time reached ({self.max_minutes_per_store}m) – stopping store cleanly")
            errors = 1
        except Exception as e:
            print(f"❌ Erreur lors du traitement du magasin {store_id}: {e}")
            self.write_store_error(store_id, store_copy.get('url'), e)
            errors = 1

        return {
            'products': store_products,
            'verified': int(bool(verified)),
            'skipped_ci': skipped_ci,
            'success': success,
            'errors': errors,
        }

    def run_shard_concurrently(self, shard_stores):
        print(f"[SHARD] Processing {len(shard_stores)} stores with concurrency={self.max_concurrency}")
        verified_stores = 0

        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            future_to_store = {executor.submit(self.process_store, store): store for store in shard_stores}
            for future in as_completed(future_to_store):
                store = future_to_store[future]
                try:
                    result = future.result()
                    self.products.extend(result.get('products', []))
                    verified_stores += result.get('verified', 0)
                    self.summary['success'] += result.get('success', 0)
                    self.summary['skipped_ci'] += result.get('skipped_ci', 0)
                    self.summary['errors'] += result.get('errors', 0)
                except Exception as e:
                    store_id = store.get('store_number', 'Unknown')
                    print(f"❌ Exception non gérée pour le magasin {store_id}: {e}")
                    self.write_store_error(store_id, store.get('url'), e)
                    self.summary['errors'] += 1

        return verified_stores

    def save_to_json(self, filename='homedepot_clearance.json'):
        """Sauvegarde en JSON"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                'total_products': len(self.products),
                'total_stores': len(self.stores),
                'scraped_at': datetime.now().isoformat(),
                'products': self.products
            }, f, ensure_ascii=False, indent=2)
        print(f"✅ JSON sauvegardé: {filename}")

    def save_to_csv(self, filename='homedepot_clearance.csv'):
        """Sauvegarde en CSV"""
        if not self.products:
            print("❌ Aucun produit à sauvegarder")
            return

        keys = set()
        for product in self.products:
            keys.update(product.keys())

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted(keys))
            writer.writeheader()
            writer.writerows(self.products)
        print(f"✅ CSV sauvegardé: {filename}")

    def print_summary(self):
        """Affiche un résumé détaillé"""
        if not self.products:
            print("❌ Aucun produit trouvé")
            return

        print("\n" + "=" * 70)
        print("📊 RÉSUMÉ DES LIQUIDATIONS PAR MAGASIN")
        print("=" * 70)

        stores_summary = {}
        for product in self.products:
            store_id = product.get('store_number', 'Unknown')
            if store_id not in stores_summary:
                stores_summary[store_id] = {
                    'name': product.get('store_name', 'Unknown'),
                    'count': 0,
                    'products': []
                }
            stores_summary[store_id]['count'] += 1
            stores_summary[store_id]['products'].append(product)

        top_stores = sorted(stores_summary.items(), key=lambda x: x[1]['count'], reverse=True)[:10]

        print(f"\n🏆 TOP 10 MAGASINS AVEC LE PLUS DE LIQUIDATIONS:")
        for i, (store_id, data) in enumerate(top_stores, 1):
            print(f"{i:2d}. {data['name']} (#{store_id}): {data['count']} produits")

        print(f"\n📦 EXEMPLES DE PRODUITS EN LIQUIDATION:")
        for i, product in enumerate(self.products[:5], 1):
            print(f"\n{i}. {product.get('name', 'Sans nom')}")
            print(f"   🏪 Magasin: {product.get('store_name')} (#{product.get('store_number')})")
            print(f"   💰 Prix: {product.get('price', 'N/A')}")
            if product.get('original_price'):
                print(f"   💵 Prix original: {product.get('original_price')}")
            if product.get('savings'):
                print(f"   💸 Économie: {product.get('savings')}")

    def print_enrich_summary(self):
        print("\n[ENRICH] summary:")
        print(f"- success: {self.summary['success']}")
        print(f"- skipped_ci: {self.summary['skipped_ci']}")
        print(f"- errors: {self.summary['errors']}")


class ShardManager:
    """Gestionnaire de shards pour distribuer les magasins"""

    def __init__(self, stores_per_shard=8):
        self.stores_per_shard = stores_per_shard
        self.shards_dir = "shards"

        if not os.path.exists(self.shards_dir):
            os.makedirs(self.shards_dir)
            print(f"📁 Dossier '{self.shards_dir}' créé")

    def create_shards(self, stores):
        """Divise les magasins en shards et sauvegarde les configurations"""
        shards = []
        total_shards = (len(stores) + self.stores_per_shard - 1) // self.stores_per_shard

        print("\n" + "=" * 70)
        print("🔧 CRÉATION DES SHARDS")
        print("=" * 70)
        print(f"Total magasins: {len(stores)}")
        print(f"Magasins par shard: {self.stores_per_shard}")
        print(f"Nombre de shards: {total_shards}")

        for i in range(0, len(stores), self.stores_per_shard):
            shard_stores = stores[i:i + self.stores_per_shard]
            shard_num = len(shards) + 1

            shard_info = {
                'shard_id': shard_num,
                'stores': shard_stores,
                'created_at': datetime.now().isoformat()
            }
            shards.append(shard_info)

            shard_filename = os.path.join(self.shards_dir, f"shard_{shard_num:02d}.json")
            with open(shard_filename, 'w', encoding='utf-8') as f:
                json.dump(shard_info, f, ensure_ascii=False, indent=2)

            print(f"✅ Shard {shard_num:02d} créé: {len(shard_stores)} magasins -> {shard_filename}")

        manifest = {
            'total_stores': len(stores),
            'stores_per_shard': self.stores_per_shard,
            'total_shards': len(shards),
            'created_at': datetime.now().isoformat(),
            'shards': [
                {
                    'shard_id': s['shard_id'],
                    'stores_count': len(s['stores']),
                    'filename': f"shard_{s['shard_id']:02d}.json"
                }
                for s in shards
            ]
        }

        manifest_filename = os.path.join(self.shards_dir, 'manifest.json')
        with open(manifest_filename, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Manifest créé: {manifest_filename}")
        return shards

    def load_shard(self, shard_id):
        """Charge un shard spécifique"""
        shard_filename = os.path.join(self.shards_dir, f"shard_{shard_id:02d}.json")

        print(f"Running shard {shard_id} using {shard_filename}")

        if not os.path.exists(shard_filename):
            print(f"❌ Shard {shard_id} introuvable: {shard_filename}")
            return None

        with open(shard_filename, 'r', encoding='utf-8') as f:
            shard_info = json.load(f)

        shard_info['filename'] = shard_filename
        print(f"Stores in shard: {len(shard_info['stores'])}")
        print(f"✅ Shard {shard_id} chargé: {len(shard_info['stores'])} magasins")
        return shard_info

    def list_shards(self):
        """Liste tous les shards disponibles"""
        manifest_filename = os.path.join(self.shards_dir, 'manifest.json')

        if not os.path.exists(manifest_filename):
            print("❌ Aucun manifest trouvé. Créez d'abord les shards avec: python script.py --create-shards")
            return

        with open(manifest_filename, 'r', encoding='utf-8') as f:
            manifest = json.load(f)

        print("\n" + "=" * 70)
        print("📋 SHARDS DISPONIBLES")
        print("=" * 70)
        print(f"Total magasins: {manifest['total_stores']}")
        print(f"Magasins par shard: {manifest['stores_per_shard']}")
        print(f"Total shards: {manifest['total_shards']}")
        print(f"Créés le: {manifest['created_at']}")
        print("\nListe des shards:")

        for shard in manifest['shards']:
            print(f"  • Shard {shard['shard_id']:02d}: {shard['stores_count']} magasins ({shard['filename']})")

        print("\n💡 Pour scraper un shard spécifique:")
        print("   python script.py --run-shard 1")
        print("   python script.py --run-shard 2")
        print("   etc.")


def log_shard_overview(shard_info):
    stores = shard_info.get('stores', [])
    if shard_info.get('filename'):
        vprint(f"Using shard file: {shard_info['filename']}")
    vprint(f"Stores in shard: {len(stores)}")
    for store in stores:
        store_id = store.get('storeId') or store.get('store_number') or 'Unknown'
        name = store.get('name') or f"Store {store_id}"
        vprint(f" - {store_id}: {name}")


def main():
    parser = argparse.ArgumentParser(
        description='Home Depot Canada Scraper avec système de shards',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:

  1. Créer les shards (à faire une seule fois):
     python home_depot_scraper.py create_shards --stores-per-shard 5

  2. Scraper un shard spécifique:
     python home_depot_scraper.py run_shard --shard 1
     python home_depot_scraper.py run_shard --shard 2
     python home_depot_scraper.py run_shard --shard 3
        """
    )

    subparsers = parser.add_subparsers(dest='command')

    create_parser = subparsers.add_parser('create_shards', help='Créer les shards')
    create_parser.add_argument('--stores-per-shard', type=int, default=8,
                               help='Nombre de magasins par shard (défaut: 8)')

    run_parser = subparsers.add_parser('run_shard', help='Scraper un shard spécifique')
    run_parser.add_argument('--shard', type=int, default=1,
                            help='Scraper le shard numéro N (défaut: 1)')

    parser.add_argument('--create-shards', action='store_true',
                       help='Créer les shards à partir de tous les magasins (compatibilité)')
    parser.add_argument('--list-shards', action='store_true',
                       help='Lister tous les shards disponibles')
    parser.add_argument('--run-shard', type=int, metavar='N',
                       help='Scraper le shard numéro N (compatibilité)')
    parser.add_argument('--stores-per-shard', type=int, default=8,
                       help='Nombre de magasins par shard (défaut: 8)')
    parser.add_argument('--verbose', action='store_true',
                        help='Activer les logs détaillés (ou utiliser VERBOSE=1)')

    args = parser.parse_args()

    global VERBOSE
    if args.verbose:
        VERBOSE = True

    selected_command = args.command
    create_shards_flag = args.create_shards or selected_command == 'create_shards'
    run_shard_id = args.run_shard if args.run_shard is not None else None
    run_shard_id = args.shard if selected_command == 'run_shard' else run_shard_id
    stores_per_shard = args.stores_per_shard
    if selected_command == 'create_shards':
        stores_per_shard = getattr(args, 'stores_per_shard', stores_per_shard)

    if not (create_shards_flag or args.list_shards or run_shard_id):
        parser.print_help()
        HomeDepotScraper().print_enrich_summary()
        return

    shard_manager = ShardManager(stores_per_shard=stores_per_shard)

    if create_shards_flag:
        scraper = HomeDepotScraper()
        stores = scraper.get_all_stores()
        shard_manager.create_shards(stores)
        print("\n✅ Shards créés avec succès!")
        print("💡 Utilisez --list-shards pour voir la liste")
        scraper.print_enrich_summary()
        return

    if args.list_shards:
        shard_manager.list_shards()
        HomeDepotScraper().print_enrich_summary()
        return

    if run_shard_id:
        print("\n" + "=" * 70)
        print(f"🚀 DÉMARRAGE DU SCRAPING - SHARD {run_shard_id}")
        print("=" * 70)

        shard_info = shard_manager.load_shard(run_shard_id)
        if not shard_info:
            scraper = HomeDepotScraper()
            scraper.print_enrich_summary()
            return

        log_shard_overview(shard_info)

        scraper = HomeDepotScraper()
        scraper.stores = shard_info['stores']

        verified_stores = scraper.run_shard_concurrently(shard_info['stores'])

        print("\n" + "=" * 70)
        print(f"✅ SHARD {run_shard_id} TERMINÉ")
        print(f"📦 {len(scraper.products)} produits trouvés")
        print(f"🏪 {verified_stores} magasins vérifiés")
        print("=" * 70)

        output_dir = "results"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        json_filename = os.path.join(output_dir, f"shard_{run_shard_id:02d}_results.json")
        csv_filename = os.path.join(output_dir, f"shard_{run_shard_id:02d}_results.csv")

        scraper.save_to_json(json_filename)
        scraper.save_to_csv(csv_filename)
        scraper.print_summary()
        scraper.print_enrich_summary()

        print("\n🎉 Script terminé avec succès!")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"⚠️  Unexpected error encountered: {exc}")
    finally:
        sys.exit(0)
