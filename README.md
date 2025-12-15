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

