Oui. Voici un **cahier des charges complet** pour le **Projet 10 : Surveillance automatique de prix et alertes** (PFM – Master SDIA). 

---

## 1) Contexte & Problématique

Les prix en ligne (e-commerce, billets, services, abonnements…) changent fréquemment. Les utilisateurs (clients, responsables achats, vendeurs) ont besoin de :

* **suivre l’évolution** de prix sur une liste de produits,
* **détecter automatiquement** les baisses/hausses significatives,
* **recevoir des alertes** (email / dashboard) au bon moment.

---

## 2) Objectifs du projet

Concevoir une chaîne complète :

1. **Collecte** automatique de prix (web scraping dynamique obligatoire : Selenium/Playwright)
2. **Stockage** historique (dataset exploitable, versionné)
3. **Analyse & Data Science** :

   * Détection d’anomalies (promotions, hausses suspectes)
   * Prévision des baisses de prix
4. **Système d’alertes** : notifications selon des règles définies
5. **Restitution** : dashboard + rapport + soutenance

---

## 3) Périmètre (Scope)

### 3.1 Sources de données (au choix)

* Sites e-commerce (ex : Jumia, Amazon, etc.) **ou**
* Sites locaux/nationaux pertinents (produits tech, électroménager, parapharmacie, etc.)

> Le choix doit respecter les conditions d’utilisation du site et limiter la charge (scraping raisonnable).

### 3.2 Ce qui est inclus

* Scraping **multi-pages + pagination**
* Gestion contenu dynamique (JS, lazy-load)
* Nettoyage, déduplication, normalisation devises/format
* Historisation (prix par date/heure)
* EDA + modèles DS/ML
* Alertes & dashboard

### 3.3 Ce qui est exclu (par défaut)

* Achat automatisé / contournement agressif anti-bot
* Scraping de données privées derrière authentification non autorisée
* Extraction massive non contrôlée

---

## 4) Utilisateurs cibles

* **Utilisateur final** : souhaite suivre un panier de produits et recevoir des alertes
* **Analyste** : consulte tendances, anomalies, prévisions
* **Administrateur** : gère la liste des URLs suivies + fréquence de collecte

---

## 5) Fonctionnalités attendues

### 5.1 Module “Configuration de suivi”

* Ajouter / supprimer un produit à suivre via :

  * URL produit
  * nom (optionnel)
  * seuils d’alerte (ex : baisse > 5%, prix < X)
  * fréquence de suivi (ex : 1/jour, 2/jour, etc.)
* Regrouper par catégorie / marque

### 5.2 Module “Scraping & Robustesse”

* Extraction minimum par produit :

  * **nom produit**
  * **prix**
  * devise
  * disponibilité (si possible)
  * vendeur (si marketplace, option)
  * date/heure de collecte
  * URL
* Gestion :

  * pagination (si liste/catégorie)
  * timeouts, retries, logs
  * changements DOM (sélecteurs robustes)
  * erreurs HTTP / pages indisponibles
* Respect des bonnes pratiques : délais entre requêtes, user-agent raisonnable

### 5.3 Module “Dataset & Historique”

* Stockage format **CSV/Parquet** (minimum exigé) + option DB (SQLite/PostgreSQL)
* Schéma de données propre (colonnes normalisées)
* Gestion doublons + valeurs manquantes
* Versioning (au minimum par date)

### 5.4 Module “Analyse Exploratoire (EDA)”

* Statistiques :

  * min/max/moyenne, volatilité, variation journalière
  * distribution des prix par catégorie/marque
* Visualisations :

  * séries temporelles par produit
  * boxplots comparatifs
  * heatmap (corrélations si features)
  * top baisses / top hausses

### 5.5 Module “Détection d’anomalies”

* Définir “anomalie” :

  * promo exceptionnelle (drop soudain)
  * hausse anormale
  * prix incohérent (scraping bug)
* Méthodes possibles :

  * règles (z-score, IQR)
  * Isolation Forest / One-Class SVM (bonus)

### 5.6 Module “Prévision / ML”

Objectif : prévoir le prix à court terme **ou** la probabilité de baisse.

* Baselines recommandées :

  * moyenne mobile / exponential smoothing
* ML (si dataset suffisant) :

  * régression (RandomForest/XGBoost/Linear)
  * modèle de classification “baisse dans 3 jours : oui/non”
* Évaluation :

  * split temporel (train -> test chronologique)
  * MAE/RMSE (régression), F1/ROC-AUC (classification)

### 5.7 Module “Alertes”

* Alertes déclenchées selon :

  * prix < seuil
  * baisse > X% sur Y jours
  * anomalie détectée
* Canaux :

  * email (minimum) **ou** console + fichier log si email non possible
  * dashboard (indicateurs “alerts”)

### 5.8 Dashboard (Restitution)

* Pages/sections :

  * vue globale (kpi : #produits, #alertes, plus fortes variations)
  * recherche + filtre (catégorie, marque)
  * détail produit (courbe prix + prédiction + anomalies)
* Outils :

  * Matplotlib/Seaborn (minimum) ou Power BI (option)
  * Streamlit (bonus très apprécié)

---

## 6) Exigences non fonctionnelles

### 6.1 Qualité & lisibilité du code

* Code structuré : `scraping/`, `cleaning/`, `analysis/`, `modeling/`, `alerts/`
* Commentaires + README d’exécution
* Gestion exceptions + logs

### 6.2 Performance

* Temps de scraping raisonnable (batch)
* Mise en cache / limitation fréquence
* Possibilité d’exécuter en “daily job” (cron / planificateur)

### 6.3 Conformité & éthique

* Respect conditions d’utilisation des sites
* Pas de collecte de données personnelles
* Débit de requêtes modéré, pas de surcharge

---

## 7) Architecture proposée (simple et claire)

1. **Scraper** (Playwright/Selenium) → export brut (raw)
2. **Nettoyage** (Pandas) → dataset final (Parquet/CSV)
3. **Analyse + ML** → outputs (figures + métriques)
4. **Alerting** → email/log + table “alerts”
5. **Dashboard** → visualisation + exploration

---

## 8) Données & format (proposition de schéma)

Table/CSV `prices_history` :

* `timestamp`
* `product_id` (hash URL)
* `product_name`
* `price_value`
* `currency`
* `availability` (option)
* `seller` (option)
* `source_site`
* `url`

Table/CSV `alerts` :

* `timestamp`
* `product_id`
* `alert_type` (threshold_drop / below_price / anomaly)
* `message`
* `price_value`

---

## 9) Plan de réalisation (phases)

**Phase 1 – Analyse du site** : pages, dynamique, API possible, anti-bot
**Phase 2 – Scraping** : extraction robuste + pagination
**Phase 3 – Nettoyage** : normalisation + dataset final
**Phase 4 – EDA** : stats + graphiques principaux
**Phase 5 – DS/ML** : anomalies + prévision + évaluation
**Phase 6 – Alertes + dashboard**
**Phase 7 – Rapport + soutenance**

---

## 10) Livrables attendus

Conformément au document :

1. **Code source complet** (.py ou .ipynb)
2. **Dataset final** (CSV/Parquet)
3. **Rapport scientifique (PDF)** : intro, méthodo, analyse, résultats, limites
4. **Présentation orale** 10–15 min 

---

## 11) Critères d’acceptation (Checklist)

* Scraper dynamique fonctionne sur au moins **N produits** (ex : ≥ 100)
* Historique de prix sur une période (ex : ≥ 7 jours ou simulation)
* Dataset propre + documentation colonnes
* Au moins **5 visualisations** pertinentes
* Détection d’anomalies opérationnelle (avec exemples)
* Prévision ou probabilité de baisse avec métriques
* Alertes démontrées (email ou logs)
* Rapport clair + conclusions critiques

---

Si tu veux, je peux aussi te donner :

* une **structure de dossier** prête à coder,
* un **template de rapport** (plan + sections),
* et une **liste de features ML** pertinentes (promos, volatilité, moyenne mobile, etc.).
