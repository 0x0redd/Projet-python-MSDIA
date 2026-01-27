# Workflow Documentation - Scraping Project

## 📋 Vue d'ensemble

Ce projet scrape des produits depuis **Jumia.ma** et **Marjanemall.ma** et les sauvegarde dans une base de données MongoDB locale.

## 🔄 Workflow Complet

### 1. Structure des Données

#### Champs communs (Jumia & Marjanemall)
- `product_id` : Identifiant unique du produit
- `name` : Nom du produit
- `price` : Prix numérique (float)
- `price_text` : Prix en texte (ex: "99.00 Dhs")
- `old_price` : Ancien prix numérique (float)
- `old_price_text` : Ancien prix en texte
- `discount` : Pourcentage de réduction (int)
- `discount_text` : Texte de réduction (ex: "-50%")
- `url` : URL complète du produit
- `image_url` : URL de l'image
- `image_alt` : Texte alternatif de l'image
- `category` : Catégorie principale
- `source` : Source du scraping ('jumia.ma' ou 'marjanemall.ma')
- `scraped_at` : Timestamp ISO du scraping
- `brand` : Marque du produit
- `rating` : Note moyenne (float ou None)
- `review_count` : Nombre d'avis (int ou None)

#### Champs spécifiques Jumia
- `categories` : Liste des catégories
- `category_key` : Clé de catégorie
- `brand_key` : Clé de marque
- `tags` : Tags du produit
- `seller_id` : ID du vendeur
- `is_official_store` : Boutique officielle
- `official_store_name` : Nom de la boutique
- `is_sponsored` : Produit sponsorisé
- `is_buyable` : Disponible à l'achat
- `express_delivery` : Livraison express
- `campaign_name` : Nom de la campagne
- `campaign_identifier` : Identifiant de campagne
- `price_euro` : Prix en euros
- `old_price_euro` : Ancien prix en euros
- `discount_euro` : Réduction en euros

#### Champs spécifiques Marjanemall
- `seller` : Nom du vendeur
- `page_number` : Numéro de page scrapée

### 2. Configuration Base de Données

**MongoDB Local** :
- Host: `localhost:27017`
- Database: `project10`
- Collections:
  - `products` : Informations des produits
  - `price_history` : Historique des prix
  - `price_changes` : Changements de prix détectés
  - `alerts` : Alertes de prix

### 3. Fichiers Principaux

#### Scrapers
- `scraping/jumia/jumia_scraper.py` : Scraper Jumia.ma (requests + BeautifulSoup)
- `scraping/marjanemall/marjanemall_scraper.py` : Scraper Marjanemall.ma (Playwright)

#### Scripts Principaux
- `main.py` : Script principal - scrape les deux sites et sauvegarde en DB
- `scraping/main.py` : Script pour scraper uniquement Jumia (fichiers CSV/Parquet)
- `scraping/main_with_db.py` : Script pour scraper Jumia avec sauvegarde DB
- `scraping/marjanemall/main.py` : Script pour scraper uniquement Marjanemall (fichiers CSV/JSON)

#### Base de Données
- `database/db_manager.py` : Gestionnaire de base de données MongoDB
- `database/quick_test.py` : Test rapide de connexion MongoDB

### 4. Chemins de Sauvegarde

**Fichiers CSV/Parquet/JSON** :
- `scraping/data/raw/` : Tous les fichiers scrapés

**Logs** :
- `logs/scraping_YYYYMMDD.log` : Logs du scraping principal
- `logs/scraping.log` : Logs du scraping Jumia
- `logs/marjanemall_scraping.log` : Logs du scraping Marjanemall

### 5. Utilisation

#### Scraper les deux sites avec DB
```bash
python main.py
python main.py --max-pages 10  # Limiter les pages
python main.py --jumia-only    # Seulement Jumia
python main.py --marjanemall-only  # Seulement Marjanemall
```

#### Scraper Jumia seul (fichiers)
```bash
python scraping/main.py --all
python scraping/main.py --category telephone-tablette
```

#### Scraper Jumia avec DB
```bash
python scraping/main_with_db.py --all
python scraping/main_with_db.py --category telephone-tablette --no-files  # DB seulement
```

#### Scraper Marjanemall seul (fichiers)
```bash
python scraping/marjanemall/main.py --all
python scraping/marjanemall/main.py --category informatique-gaming --max-pages 10
```

#### Tester la connexion DB
```bash
python database/quick_test.py
```

## ✅ Vérifications de Cohérence

### ✅ Imports
- Tous les fichiers utilisent les bons chemins d'import
- `from scraping.jumia.jumia_scraper import JumiaScraper`
- `from scraping.marjanemall.marjanemall_scraper import MarjanemallScraper`

### ✅ Structure de Données
- MarjanemallScraper normalise les données pour correspondre à JumiaScraper
- Tous les champs requis par DatabaseManager sont présents
- `scraped_at` utilisé partout (compatible avec DB)

### ✅ Base de Données
- Connexion locale : `localhost:27017`
- Database : `project10`
- Collections créées automatiquement

### ✅ Chemins
- Tous les fichiers sauvegardent dans `scraping/data/raw/`
- Logs dans `logs/`
- Création automatique des répertoires

## 🔍 Points d'Attention

1. **MongoDB doit être démarré** avant d'exécuter les scripts avec DB
2. **Playwright** doit être installé pour Marjanemall : `pip install playwright && playwright install chromium`
3. **Structure de données** : Les deux scrapers retournent maintenant des structures compatibles
4. **Timestamps** : Utiliser `scraped_at` (format ISO) pour compatibilité DB
