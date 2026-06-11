# 📘 Runbook Opérationnel - Plateforme de Données Universitaire

Ce document centralise le cycle de vie du code (Workflow Git), les procédures d'initialisation de l'infrastructure et le guide de résolution des incidents.

---

## 👥 1. Workflow Git & Collaboration en Équipe

Pour maintenir un code propre et éviter d'écraser le travail de vos collaborateurs, l'usage de la branche `main` en direct est formellement interdit. Suivez scrupuleusement ce cycle :

### Étape 1 : Cloner le projet (Première arrivée)
Ouvrez votre terminal PowerShell dans le dossier de votre choix et récupérez le dépôt GitHub :
```powershell
git clone https://github.com
cd university-data-platform
```

### Étape 2 : Créer votre branche de travail personnelle
Avant de coder, créez et basculez sur une branche dédiée à la tâche qui vous est attribuée. Utilisez des préfixes clairs (`feature/`, `bugfix/`, `refactor/`) suivis de votre nom et de la tâche :
```powershell
# Syntaxe : git checkout -b feature/nom-description-tache
git checkout -b feature/nom-ingestion-etudiants
```
*Vérifiez à tout moment sur quelle branche vous vous trouvez via la commande : `git branch`*

### Étape 3 : Valider vos modifications (Commit)
Une fois vos fichiers modifiés et validés localement, indexez-les et créez un point de sauvegarde (commit) avec un message descriptif en anglais ou en français :
```powershell
# 1. Indexer les fichiers modifiés (ex: metadata.py)
git add jobs/common/metadata.py

# 2. Enregistrer le commit
git commit -m "feat(common): ajoute le schéma de validation des étudiants dans metadata"
```

### Étape 4 : Publier votre branche sur GitHub (Push)
La toute première fois que vous envoyez votre nouvelle branche sur GitHub, vous devez définir la branche distante de référence avec l'option `-u` (Upstream) :
```powershell
git push -u origin feature/nom-ingestion-etudiants
```
*Pour les commits suivants sur cette même branche, un simple `git push` suffira.*

### Étape 5 : Intégration finale (Pull Request)
Une fois votre développement terminé et testé localement :
1. Rendez-vous sur l'interface web de GitHub.
2. Ouvrez une **Pull Request (PR)** de votre branche vers la branche `main`.
3. Demandez la revue d'au moins un autre membre de l'équipe Data Engineering.
4. Une fois validée et les tests CI/CD au vert, fusionnez le code.

---

## 🚀 2. Déploiement et Initialisation (Premier Lancement)

À exécuter immédiatement après le clonage du projet.

### Étape 1 : Amorçage Windows (Téléchargement automatique des JARs et Binaires)
Ouvrez PowerShell à la racine du projet et exécutez le script d'amorçage :
```powershell
.\scripts\bootstrap.ps1
```
*Note : Si l'exécution est bloquée par Windows, autorisez-la temporairement via :*
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process`

### Étape 2 : Lancement complet des 13 services de l'infrastructure
```powershell
docker compose --env-file .env.dev -f docker-compose.dev.yml up --build -d
```

### Étape 3 : Cartographie des Accès Web Locaux
* 📊 **Orchestrateur (Airflow)** : `http://localhost:8082` (Identifiants : `admin` / `admin`)
* ⚙️ **Moteur de calcul (Spark)** : `http://localhost:8080` (Interface Master)
* 🪣 **Data Lake (MinIO)** : `http://localhost:9001` (Identifiants : `admin_dev` / `password_dev_123`)
* 🔍 **Search API (Swagger)** : `http://localhost:8000/docs`
* 📉 **Business Intelligence (Metabase)** : `http://localhost:3000`

---