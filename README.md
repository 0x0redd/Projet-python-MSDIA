# Projet 10 : Surveillance automatique de prix et alertes

**PFM – Master SDIA**

## Description

Système complet de surveillance automatique des prix en ligne avec :
- Collecte automatique de prix via web scraping dynamique
- Stockage historique des données
- Analyse exploratoire et détection d'anomalies
- Modèles de prévision des baisses de prix
- Système d'alertes (email/dashboard)
- Dashboard interactif

## Structure du Projet

```
Projet-python-MSDIA/
├── scraping/          # Module de web scraping
├── cleaning/          # Module de nettoyage des données
├── analysis/          # Module d'analyse exploratoire (EDA)
├── modeling/          # Module de modélisation ML
├── alerts/            # Module de gestion des alertes
├── dashboard/         # Module du dashboard Streamlit
├── config/            # Fichiers de configuration
├── data/              # Données (raw, processed)
│   ├── raw/          # Données brutes
│   └── processed/    # Données nettoyées
├── outputs/           # Résultats (figures, rapports)
├── logs/              # Fichiers de logs
├── requirements.txt   # Dépendances Python
├── setup.md          # Guide de configuration
└── README.md         # Ce fichier
```

## Installation

Consultez le fichier [setup.md](setup.md) pour les instructions détaillées de configuration de l'environnement virtuel.

### Installation rapide

```bash
# Créer l'environnement virtuel
python -m venv venv

# Activer l'environnement
# Windows (PowerShell):
.\venv\Scripts\Activate.ps1
# Linux/macOS:
source venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt

# Installer les navigateurs Playwright (si utilisé)
playwright install
```

## Utilisation

### 1. Configuration

Créez un fichier `.env` dans le dossier `config/` avec vos paramètres :
- URLs des sites à scraper
- Paramètres d'alerte
- Configuration email (si applicable)

### 2. Scraping

```bash
python scraping/main.py
```

### 3. Nettoyage des données

```bash
python cleaning/main.py
```

### 4. Analyse exploratoire

```bash
python analysis/main.py
```

### 5. Modélisation

```bash
python modeling/main.py
```

### 6. Dashboard

```bash
streamlit run dashboard/app.py
```

## Technologies Utilisées

- **Web Scraping** : Selenium, Playwright, BeautifulSoup
- **Data Processing** : Pandas, NumPy
- **Data Science** : Scikit-learn, SciPy
- **Visualization** : Matplotlib, Seaborn, Plotly
- **Dashboard** : Streamlit
- **Storage** : CSV, Parquet, SQLite (optionnel)

## Livrables

1. Code source complet
2. Dataset final (CSV/Parquet)
3. Rapport scientifique (PDF)
4. Présentation orale (10-15 min)

## Auteurs

Équipe du projet PFM - Master SDIA

## Licence

Ce projet est réalisé dans le cadre académique du Master SDIA.
