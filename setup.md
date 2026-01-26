# Guide de Configuration de l'Environnement Virtuel

Ce guide vous explique comment configurer l'environnement virtuel Python pour le projet **Surveillance automatique de prix et alertes**.

## Prérequis

- Python 3.9 ou supérieur installé sur votre système
- pip (gestionnaire de paquets Python)

### Vérifier l'installation de Python

```bash
python --version
# ou
python3 --version
```

## Configuration de l'Environnement Virtuel

### Windows (PowerShell)

1. **Créer l'environnement virtuel**

```powershell
# Depuis le répertoire racine du projet
python -m venv venv
```

2. **Activer l'environnement virtuel**

```powershell
.\venv\Scripts\Activate.ps1
```

Si vous obtenez une erreur d'exécution de script, exécutez d'abord :
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

3. **Mettre à jour pip**

```powershell
python -m pip install --upgrade pip
```

4. **Installer les dépendances**

```powershell
pip install -r requirements.txt
```

5. **Installer les navigateurs pour Playwright** (si vous utilisez Playwright)

```powershell
playwright install
```

### Windows (Command Prompt / CMD)

1. **Créer l'environnement virtuel**

```cmd
python -m venv venv
```

2. **Activer l'environnement virtuel**

```cmd
venv\Scripts\activate.bat
```

3. **Mettre à jour pip**

```cmd
python -m pip install --upgrade pip
```

4. **Installer les dépendances**

```cmd
pip install -r requirements.txt
```

5. **Installer les navigateurs pour Playwright**

```cmd
playwright install
```

### Linux / macOS

1. **Créer l'environnement virtuel**

```bash
python3 -m venv venv
```

2. **Activer l'environnement virtuel**

```bash
source venv/bin/activate
```

3. **Mettre à jour pip**

```bash
python -m pip install --upgrade pip
```

4. **Installer les dépendances**

```bash
pip install -r requirements.txt
```

5. **Installer les navigateurs pour Playwright**

```bash
playwright install
```

## Vérification de l'Installation

Après l'installation, vérifiez que tout fonctionne :

```bash
python -c "import pandas; import selenium; import playwright; print('Installation réussie!')"
```

## Utilisation Quotidienne

### Activer l'environnement virtuel

**Windows (PowerShell):**
```powershell
.\venv\Scripts\Activate.ps1
```

**Windows (CMD):**
```cmd
venv\Scripts\activate.bat
```

**Linux/macOS:**
```bash
source venv/bin/activate
```

### Désactiver l'environnement virtuel

```bash
deactivate
```

## Structure des Dépendances

Les dépendances sont organisées par catégorie dans `requirements.txt` :

- **Web Scraping** : requests, beautifulsoup4, selenium, playwright
- **Data Processing** : pandas, numpy, pyarrow
- **Data Science & ML** : scikit-learn, scipy
- **Visualization** : matplotlib, seaborn, plotly
- **Dashboard** : streamlit
- **Utilities** : python-dotenv, tqdm, loguru

## Dépannage

### Problème : "pip n'est pas reconnu"

Solution : Utilisez `python -m pip` au lieu de `pip`

### Problème : Erreur lors de l'installation de Playwright

Solution : Installez les navigateurs séparément :
```bash
playwright install chromium
```

### Problème : Conflits de versions

Solution : Créez un nouvel environnement virtuel et réinstallez :
```bash
# Supprimer l'ancien venv
rm -rf venv  # Linux/macOS
rmdir /s venv  # Windows

# Recréer
python -m venv venv
# Puis réactiver et réinstaller
```

### Problème : Permissions insuffisantes (Linux/macOS)

Solution : Utilisez `--user` si nécessaire :
```bash
pip install --user -r requirements.txt
```

## Notes Importantes

- **Toujours activer l'environnement virtuel** avant de travailler sur le projet
- **Ne pas commiter** le dossier `venv/` dans Git (déjà dans `.gitignore`)
- **Mettre à jour** `requirements.txt` si vous ajoutez de nouvelles dépendances :
  ```bash
  pip freeze > requirements.txt
  ```

## Prochaines Étapes

Une fois l'environnement configuré :

1. Créez un fichier `.env` pour vos configurations (voir `config/.env.example`)
2. Configurez vos paramètres de scraping dans `config/config.py`
3. Consultez le `README.md` pour démarrer le projet
