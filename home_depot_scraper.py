import argparse
import csv
import json
import os
import random
import re
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


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
                    self.update_headers()
                    wait_time = min(2 ** attempt + random.uniform(1, 3), 30)
                    print(f"⏳ Attente de {wait_time:.1f}s avant nouvelle tentative...")
                    time.sleep(wait_time)

                print(f"🔍 Requête: {url[:80]}... (Tentative {attempt + 1}/{max_retries})")

                response = self.session.get(url, timeout=30)

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

        store_locator_url = f"{self.base_url}/en/store-directory"
        response = self.make_request(store_locator_url)

        if response:
            soup = BeautifulSoup(response.content, 'html.parser')
            store_links = soup.find_all('a', href=re.compile(r'/store-details/'))

            for link in store_links:
                store_info = {
                    'url': urljoin(self.base_url, link['href']),
                    'name': link.get_text(strip=True)
                }
                match = re.search(r'/(\d{4})$', link['href'])
                if match:
                    store_info['store_number'] = match.group(1)
                self.stores.append(store_info)

        if not self.stores:
            print("⚠️  Utilisation de la méthode de fallback...")
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
            products = soup.find_all(['div', 'article'], class_=lambda x: x and ('product' in x.lower() or 'pod' in x.lower()) if x else False)

            for product_elem in products:
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

    args = parser.parse_args()

    selected_command = args.command
    create_shards_flag = args.create_shards or selected_command == 'create_shards'
    run_shard_id = args.run_shard if args.run_shard is not None else None
    run_shard_id = args.shard if selected_command == 'run_shard' else run_shard_id
    stores_per_shard = args.stores_per_shard
    if selected_command == 'create_shards':
        stores_per_shard = getattr(args, 'stores_per_shard', stores_per_shard)

    if not (create_shards_flag or args.list_shards or run_shard_id):
        parser.print_help()
        return

    shard_manager = ShardManager(stores_per_shard=stores_per_shard)

    if create_shards_flag:
        scraper = HomeDepotScraper()
        stores = scraper.get_all_stores()
        shard_manager.create_shards(stores)
        print("\n✅ Shards créés avec succès!")
        print("💡 Utilisez --list-shards pour voir la liste")
        return

    if args.list_shards:
        shard_manager.list_shards()
        return

    if run_shard_id:
        print("\n" + "=" * 70)
        print(f"🚀 DÉMARRAGE DU SCRAPING - SHARD {run_shard_id}")
        print("=" * 70)

        shard_info = shard_manager.load_shard(run_shard_id)
        if not shard_info:
            return

        scraper = HomeDepotScraper()
        scraper.stores = shard_info['stores']

        verified_stores = scraper.scrape_shard(shard_info['stores'])

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

        print("\n🎉 Script terminé avec succès!")


if __name__ == "__main__":
    main()
