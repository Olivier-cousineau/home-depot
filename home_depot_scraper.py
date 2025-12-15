import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime
import csv
import random
from urllib.parse import urljoin, quote
import re


class HomeDepotScraper:
    def __init__(self):
        self.base_url = "https://www.homedepot.ca"
        self.api_base = "https://www.homedepot.ca/api"

        # Rotation des User-Agents pour éviter la détection
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

        self.session = requests.Session()
        self.products = []
        self.stores = []
        self.update_headers()

    def update_headers(self):
        """Met à jour les headers avec un User-Agent aléatoire"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'fr-CA,fr;q=0.9,en-CA;q=0.8,en;q=0.7',
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

    def make_request(self, url, max_retries=5, use_json=False):
        """Effectue une requête avec retry et rotation de User-Agent"""
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    self.update_headers()  # Change User-Agent à chaque retry
                    wait_time = min(2 ** attempt + random.uniform(1, 3), 30)
                    print(f"⏳ Attente de {wait_time:.1f}s avant nouvelle tentative...")
                    time.sleep(wait_time)

                print(f"🔍 Requête: {url[:80]}... (Tentative {attempt + 1}/{max_retries})")

                response = self.session.get(url, timeout=30)

                # Détection de CAPTCHA
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

            if attempt == max_retries - 1:
                print(f"❌ Échec après {max_retries} tentatives")
                return None

        return None

    def get_all_stores(self):
        """Récupère la liste de tous les magasins Home Depot au Canada"""
        print("\n" + "=" * 70)
        print("📍 RÉCUPÉRATION DES MAGASINS HOME DEPOT CANADA")
        print("=" * 70)

        # Liste des provinces canadiennes
        provinces = {
            'AB': 'Alberta',
            'BC': 'British Columbia',
            'MB': 'Manitoba',
            'NB': 'New Brunswick',
            'NL': 'Newfoundland and Labrador',
            'NS': 'Nova Scotia',
            'ON': 'Ontario',
            'PE': 'Prince Edward Island',
            'QC': 'Quebec',
            'SK': 'Saskatchewan'
        }

        # Tentative via l'API de localisation
        store_locator_url = f"{self.base_url}/en/store-directory"
        response = self.make_request(store_locator_url)

        if response:
            soup = BeautifulSoup(response.content, 'html.parser')

            # Recherche des liens vers les magasins
            store_links = soup.find_all('a', href=re.compile(r'/store-details/'))

            for link in store_links:
                store_info = {
                    'url': urljoin(self.base_url, link['href']),
                    'name': link.get_text(strip=True)
                }

                # Extraction du numéro de magasin depuis l'URL
                match = re.search(r'/(\d{4})$', link['href'])
                if match:
                    store_info['store_number'] = match.group(1)

                self.stores.append(store_info)

        # Fallback: génération de numéros de magasins communs
        if not self.stores:
            print("⚠️  Utilisation de la méthode de fallback...")
            # Numéros de magasins typiques au Canada (7000-7300)
            for store_num in range(7001, 7300):
                self.stores.append({
                    'store_number': str(store_num),
                    'url': f"{self.base_url}/store-details/{store_num}"
                })

        print(f"✅ {len(self.stores)} magasins identifiés")
        return self.stores

    def verify_store(self, store):
        """Vérifie si un magasin existe et récupère ses détails"""
        response = self.make_request(store['url'])
        if response and response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            # Récupération des détails du magasin
            store_name = soup.find(['h1', 'h2'], class_=lambda x: x and 'store' in x.lower() if x else False)
            if store_name:
                store['name'] = store_name.get_text(strip=True)
                store['verified'] = True
                return True

        store['verified'] = False
        return False

    def scrape_clearance_for_store(self, store):
        """Scrape les produits en liquidation pour un magasin spécifique"""
        print(f"\n🏪 Magasin: {store.get('name', store.get('store_number'))}")

        # URLs possibles pour les liquidations
        clearance_urls = [
            f"{self.base_url}/en/search?q=clearance&storeId={store['store_number']}",
            f"{self.base_url}/fr/recherche?q=liquidation&storeId={store['store_number']}",
            f"{self.base_url}/en/deals/clearance?storeId={store['store_number']}",
        ]

        store_products = []

        for url in clearance_urls:
            response = self.make_request(url)
            if not response:
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            # Recherche des produits
            products = soup.find_all(['div', 'article'], class_=lambda x: x and ('product' in x.lower() or 'pod' in x.lower()) if x else False)

            for product_elem in products:
                product_info = self.extract_product_info(product_elem, store)
                if product_info:
                    store_products.append(product_info)

            if products:
                break  # Si on trouve des produits, pas besoin d'essayer les autres URLs

        print(f"   ✓ {len(store_products)} produits en liquidation trouvés")
        return store_products

    def extract_product_info(self, product_elem, store):
        """Extrait les informations d'un produit"""
        try:
            product_data = {
                'store_number': store.get('store_number'),
                'store_name': store.get('name', 'Unknown')
            }

            # Nom du produit
            name_elem = product_elem.find(['h3', 'h2', 'span', 'a'], class_=lambda x: x and ('title' in x.lower() or 'name' in x.lower()) if x else False)
            if not name_elem:
                name_elem = product_elem.find('a', class_=lambda x: x and 'product' in x.lower() if x else False)
            if name_elem:
                product_data['name'] = name_elem.get_text(strip=True)

            # SKU
            sku_elem = product_elem.find(['span', 'div'], string=re.compile(r'(SKU|Model):', re.I))
            if sku_elem:
                sku_text = sku_elem.get_text(strip=True)
                sku_match = re.search(r'(?:SKU|Model):\s*(\S+)', sku_text, re.I)
                if sku_match:
                    product_data['sku'] = sku_match.group(1)

            # Prix actuel
            price_elem = product_elem.find(['span', 'div'], class_=lambda x: x and 'price' in x.lower() if x else False)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                product_data['price'] = price_text

            # Prix original / Was Price
            was_price_elem = product_elem.find(['span', 'div'], class_=lambda x: x and ('was' in x.lower() or 'original' in x.lower()) if x else False)
            if was_price_elem:
                product_data['original_price'] = was_price_elem.get_text(strip=True)

            # Rabais / Économies
            save_elem = product_elem.find(['span', 'div'], class_=lambda x: x and 'save' in x.lower() if x else False)
            if save_elem:
                product_data['savings'] = save_elem.get_text(strip=True)

            # URL du produit
            link_elem = product_elem.find('a', href=True)
            if link_elem:
                product_data['url'] = urljoin(self.base_url, link_elem['href'])

            # Image
            img_elem = product_elem.find('img')
            if img_elem:
                product_data['image'] = img_elem.get('src', img_elem.get('data-src', ''))

            # Badge clearance/liquidation
            badge_elem = product_elem.find(['span', 'div'], class_=lambda x: x and ('clearance' in x.lower() or 'liquidation' in x.lower()) if x else False)
            if badge_elem:
                product_data['clearance_badge'] = badge_elem.get_text(strip=True)

            # Disponibilité en magasin
            stock_elem = product_elem.find(['span', 'div'], string=re.compile(r'(in stock|en stock|available)', re.I))
            if stock_elem:
                product_data['availability'] = stock_elem.get_text(strip=True)

            product_data['scraped_at'] = datetime.now().isoformat()

            # Ne retourne que si on a au moins un nom
            return product_data if product_data.get('name') else None

        except Exception as e:
            print(f"⚠️  Erreur extraction produit: {e}")
            return None

    def scrape_all(self, max_stores=None):
        """Lance le scraping complet de tous les magasins"""
        print("\n" + "=" * 70)
        print("🚀 DÉMARRAGE DU SCRAPER HOME DEPOT CANADA")
        print("=" * 70)

        # Récupération des magasins
        self.get_all_stores()

        if max_stores:
            stores_to_scrape = self.stores[:max_stores]
            print(f"⚙️  Mode test: scraping de {max_stores} magasins seulement")
        else:
            stores_to_scrape = self.stores

        # Vérification et scraping par magasin
        verified_stores = 0
        for i, store in enumerate(stores_to_scrape, 1):
            print(f"\n📊 Progression: {i}/{len(stores_to_scrape)}")

            # Vérification périodique du magasin
            if i % 10 == 0 or not store.get('verified'):
                if self.verify_store(store):
                    verified_stores += 1

            # Scraping des liquidations
            store_products = self.scrape_clearance_for_store(store)
            self.products.extend(store_products)

            # Pause plus longue tous les 5 magasins
            if i % 5 == 0:
                print("⏸️  Pause de sécurité...")
                self.smart_delay(10, 15)

        print("\n" + "=" * 70)
        print("✅ SCRAPING TERMINÉ")
        print(f"📦 {len(self.products)} produits en liquidation trouvés")
        print(f"🏪 {verified_stores} magasins vérifiés")
        print("=" * 70)

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

        # Grouper par magasin
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

        # Top 10 magasins avec le plus de liquidations
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
            if product.get('url'):
                print(f"   🔗 {product.get('url')}")


# Utilisation
if __name__ == "__main__":
    scraper = HomeDepotScraper()

    # Mode test: scraper seulement 5 magasins
    # scraper.scrape_all(max_stores=5)

    # Mode complet: tous les magasins
    scraper.scrape_all()

    scraper.print_summary()
    scraper.save_to_json()
    scraper.save_to_csv()

    print("\n🎉 Script terminé avec succès!")
