# 📑 Guide de Workflow - Processus d'Ingestion (Bronze Layer)

Ce document décrit le processus obligatoire que chaque membre de l'équipe doit suivre pour implémenter un nouveau flux d'ingestion de données universitaires. Aucun code ne sera validé en Pull Request s'il dévie de ce workflow.

---

## 🧭 Vue d'ensemble du Processus

L'objectif de l'ingestion est de collecter la donnée source et de la déposer **sans aucune modification métier** dans la couche **Raw (Bronze)** du Data Lake MinIO, tout en assurant l'observabilité.

```text
[Source: API / Web / PDF / JSON / IMAGES] 
      │
      ▼ (Étape 1 & 2 : Variables & Schéma)
[ jobs/common/config.py & metadata.py ]
      │
      ▼ (Étape 3 : Transport sécurisé en mémoire)
[ jobs/common/minio_client.py ]
      │
      ▼ (Étape 4 : Script modulaire)
[ jobs/ingestion/extract_xxx.py ]
      │
      ▼ (Étape 5 : Orchestration unique)
[ jobs/ingestion/run.py ]
```

---

## les 3 etapes obligatoirs  d'ingestion

### Etape 1 : voir jobs/common/config.py
ce fichier nous permet de charger tout les variables d'environnement car on l'a besoin 
sur l'etape 2

### Etape 2 : utilistation de jobs/common/minio_client.py
Les scripts d'ingestion ne doivent jamais instancier le SDK `boto3` directement. 
Par exemple : Si votre source fournit du flux fluide (JSON), vous devez obligatoirement utiliser la fonction **`minio_client.upload_json()`** pour transférer la donnée directement depuis la mémoire vive.

### Étape 3 : Implémentation du Script Métier dans `jobs/ingestion/`
Créez un fichier isolé et spécialisé par cas d'usage (ex: `jobs/ingestion/extract_json.py`).
*   **Traces** : Intégrez la bibliothèque `loguru` de la couche common pour historiser chaque action (`logger.info`, `logger.warning`).
*   **Gestion des pannes** : Utilisez les exceptions personnalisées de **`jobs/common/exceptions.py`** (ex: `raise SourceAPIHTTPError`) pour qu'Airflow puisse comprendre la cause d'un éventuel crash.
* **Remarque**:
Les "sous-dossiers" (fsac/, fsbm/, etc.), ils apparaîtront automatiquement lors du premier chargement de données pendant le développement ou l'exécution des pipelines.

---

# 🧪 Guide de Validation - Test et Déploiement des Flux d'Ingestion

Cet partie décrit la procédure standardisée en 5 étapes pour tester, valider et commiter un nouveau pipeline d'ingestion direct en mémoire vive (RAM) vers le Data Lake, en garantissant l'isolation des environnements de Développement (`.env.dev`) et de Production (`.env.prod`).

---

## 👥 Principe d'Isolation (Sandbox)

Chaque développeur configure son propre espace de stockage isolé via son fichier local `.env.dev` (exclu de Git via `.gitignore`). Le code Python reste générique et immuable en consommant uniquement la variable `settings.MINIO_RAW_BUCKET`.

*   **En Développement (`.env.dev`)** : `MINIO_RAW_BUCKET_WEB=raw-web-html-dev` (ou tout autre suffixe propre à votre tâche `xx`).
*   **En Production (`.env.prod`)** : `MINIO_RAW_BUCKET_WEB=raw-web-html-prod` (Verrouillé sur le Cloud de l'université).

### Etape 1 : docker compose --env-file .env.dev -f docker-compose.dev.yml up -d

### Étape 2 : Exécution du Job en Mode Isolé
Forcez l'exécution de votre script spécifique directement à l'intérieur du conteneur d'ingestion actif pour valider son comportement unitaire :
```powershell
docker exec -it <nom d'image> python -m <package(chemein du script)>
docker exec -it university-data-platform_ingestion python -m jobs.ingestion.extract_xxx
```

### Étape 3 : Vérification du Data Lake Local (MinIO DEV)
1. Connectez-vous sur la console graphique locale de MinIO : `http://localhost:9001` (username / password).
2. Accédez à l'onglet **Buckets**.
3. **Validation** : Vérifiez visuellement que votre bucket personnalisé (ex: `BUCKET-DEV-DOCUMENTS`) contient le fichier ingéré au bon emplacement (ex: `raw-documents-dev/<faculty_name>/file.pdf`)

### Étape 4 : Vérification de la Gouvernance des Métadonnées
1. Ouvrez votre fichier de logs persistant généré automatiquement sur votre machine : `logs/ingestion/ingestion_run.log`.
2. Vérifiez que la bibliothèque `loguru` affiche un statut `SUCCESS` avec l'horodatage précis.
3. Assurez-vous qu'aucune exception de type `DataQualityValidationError` ou `SourceAPIHTTPError` n'a été levée dans les traces.

### Étape 5 : Validation et Publication Git (Commit & Push)
Une fois les étapes 4 et 5 validées avec succès, le code est déclaré stable et prêt pour la revue de code.
```powershell
# 1. Indexer uniquement les fichiers de code créés (Jamais les fichiers .env)
git add jobs/ingestion/extract_xxx.py

# 2. Créer le point de sauvegarde local
git commit -m "feat(ingestion): implémente l'extraction directe en mémoire pour la source xxx"

# 3. Pousser la mise à jour sur votre branche de feature GitHub
git push origin feature/votre-nom-tache
```
---
⚠️ **Rappel Sécurité CI/CD** : Lors du déploiement automatisé en Production par les outils de Jenkins/GitHub Actions, le serveur ignorera le fichier d'amorçage local et injectera de manière étanche le fichier `.env.prod`. Vos buckets Cloud de production se rempliront automatiquement sans aucune modification de votre code source

---

