# home-depot

## Workflow pour lancer chaque shard manuellement

Ce référentiel contient un scraper Home Depot Canada qui répartit les magasins en *shards* (lots) pour exécuter le scraping magasin par magasin. Suivez le flux ci‑dessous pour créer puis lancer chaque shard à la demande.

1. **Créer les shards (une seule fois ou après modification de `--stores-per-shard`)**
   ```bash
   python home_depot_scraper.py --create-shards --stores-per-shard 8
   ```
   *Génère un dossier `shards/` contenant un fichier `manifest.json` et un fichier `shard_XX.json` par lot de magasins.*

2. **Lister les shards disponibles et vérifier leur taille**
   ```bash
   python home_depot_scraper.py --list-shards
   ```
   *Affiche le nombre total de magasins, la taille des shards et les identifiants disponibles (1, 2, 3, ...).* 

3. **Lancer manuellement un shard spécifique**
   ```bash
   python home_depot_scraper.py --run-shard <ID>
   ```
   Remplacez `<ID>` par le numéro du shard listé à l'étape précédente (ex. `--run-shard 1`). Chaque exécution crée un dossier `results/` avec:
   - `shard_<ID>_results.json`
   - `shard_<ID>_results.csv`

4. **Reprendre ou distribuer l'exécution shard par shard**
   - Exécutez `--run-shard` autant de fois que nécessaire pour couvrir tous les shards.
   - Les shards sont indépendants : vous pouvez les lancer en parallèle sur plusieurs machines ou séquentiellement.

5. **Conseils pratiques**
   - Si aucune shard n'apparaît avec `--list-shards`, recréez-les avec `--create-shards`.
   - Ajustez `--stores-per-shard` si vous souhaitez des lots plus petits ou plus grands, puis relancez `--create-shards`.
   - Les journaux indiquent la progression du shard, les vérifications de magasins et les sauvegardes de fichiers.

## Lancer un shard via GitHub Actions (workflow manuel)

Un workflow GitHub Actions est disponible pour déclencher ces commandes sans ligne de commande locale. Dans l'onglet **Actions** de votre dépôt GitHub :

1. Sélectionnez le workflow **"Manual shard runner"**.
2. Cliquez sur **"Run workflow"** et choisissez l'action à exécuter :
   - `create_shards` : génère les shards avec `stores_per_shard` (défaut : 8).
   - `list_shards` : affiche dans les logs la liste des shards disponibles.
   - `run_shard` : lance un shard spécifique (champ `shard_id` requis).
3. Une fois le workflow terminé, récupérez les fichiers générés dans l'onglet **Artifacts** :
   - `shards` après `create_shards` (manifest.json et fichiers shard_XX.json).
   - `shard-<ID>-results` après `run_shard` (résultats CSV/JSON du shard lancé).

