#  Pipeline de Publications Académiques — OpenAlex → Data Lakehouse → Elasticsearch

##  Vue d'ensemble

Ce projet implémente un pipeline **ETL Big Data** en trois étapes, permettant de collecter les publications scientifiques d'une université depuis l'API **OpenAlex**, de les transformer et de les stocker dans un **Data Lakehouse (Apache Hudi)**, puis de les indexer dans **Elasticsearch** pour une recherche sémantique performante.

```
┌──────────────────┐     ┌───────────────────────┐     ┌────────────────────────┐
│   OpenAlex API    │ ──▶ │   Bronze → Silver     │ ──▶ │   Elasticsearch Index   │
│  (Extraction)     │     │  (Transformation Spark)│     │      (Indexation)      │
└──────────────────┘     └───────────────────────┘     └────────────────────────┘
   extract_openalex.py     transform_publications.py       index_publications.py
```

Le pipeline suit une architecture **Medallion (Bronze / Silver)** :
- **Bronze** : données brutes JSON stockées telles quelles (MinIO)
- **Silver** : données nettoyées, normalisées et validées (table Hudi + snapshot Parquet)
- **Search Layer** : index Elasticsearch pour la recherche full-text/sémantique

---

##  Composants du pipeline

### 1️`extract_openalex.py` — Extraction des données

**Rôle** : Récupérer l'intégralité des publications scientifiques rattachées à une institution donnée via l'API publique [OpenAlex](https://openalex.org).

**Fonctionnement** :
- Utilise l'endpoint `https://api.openalex.org/works`
- Filtre les résultats par identifiant d'institution OpenAlex (ex. `I99297268`)
- Implémente une **pagination par curseur** (`cursor`) pour parcourir l'ensemble des résultats, avec une taille de page de **200 enregistrements**
- Fonctionne comme un **générateur Python** (`yield`), ce qui permet de traiter les publications au fil de l'eau sans charger toutes les données en mémoire
- S'arrête automatiquement lorsque l'API ne renvoie plus de résultats ou de curseur suivant

**Signature principale** :
```python
fetch_openalex_publications(institution_id: str) -> generator
```

**Exemple d'utilisation** :
```python
for publication in fetch_openalex_publications("I99297268"):
    # traiter / sauvegarder chaque publication (ex. écriture JSON brute dans MinIO)
    ...
```

---

### 2️⃣ `transform_publications.py` — Transformation Bronze → Silver (Apache Spark)

**Rôle** : Lire les fichiers JSON bruts déposés dans le bucket **MinIO Bronze**, les nettoyer, les normaliser, les valider, puis les écrire dans une table **Apache Hudi** (couche Silver) ainsi qu'un snapshot **Parquet** partitionné.

**Étapes du job Spark** :

| Étape | Description |
|-------|-------------|
| 1. Résolution du chemin projet | Localise dynamiquement le dossier `jobs/` pour construire les imports (compatible exécution depuis n'importe quel répertoire) |
| 2. Listing des fichiers | Sépare les fichiers **RAW** (`*.json`) des fichiers **metadata** (`*.metadata.json`) |
| 3. Lecture texte brute | Recompose chaque fichier JSON ligne par ligne (`spark.read.text`) et le regroupe par `_file_key` |
| 4. Parsing double format | Gère deux formats OpenAlex possibles : objet direct **et** wrapper `{"results": [...]}` (via `from_json` + `explode`) |
| 5. Normalisation | Extrait les champs clés (titre, DOI, année, auteurs, topic, domaine, citations, accès ouvert, etc.) via `_normalize_openalex()` |
| 6. Jointure metadata | Enrichit les données avec les métadonnées de crawl (`record_id`, `crawl_timestamp`, `payload_checksum`) |
| 7. Mapping Silver | Sélectionne et renomme les colonnes selon le schéma cible `SILVER_PUBLICATIONS_SCHEMA` |
| 8. Nettoyage | Application de `clean_publications()` (règles de qualité de données) |
| 9. Texte normalisé | Construction d'un champ `normalized_text` (concaténation title + auteurs + topic + domaine, etc.) pour l'indexation sémantique, mis en minuscules et sans espaces multiples |
| 10. Validation | Vérifie l'absence de valeurs nulles et l'unicité sur les clés métier (`BUSINESS_KEYS`) |
| 11. Écriture Hudi | Écrit la table `research_publications` en mode **append**, partitionnée par `publication_year` et `university_name` |
| 12. Snapshot Parquet | Sauvegarde un snapshot complet (mode `overwrite`) dans le bucket **curated** |

**Champs extraits par publication** :
`id`, `doi`, `title`, `publication_year`, `publication_date`, `language`, `journal_name`, `publication_type`, `authors`, `primary_topic`, `subfield`, `field`, `domain`, `is_open_access`, `cited_by_count`

**Table de destination (Hudi)** :
- **Nom** : `silver.research_publications`
- **Clé d'enregistrement (`record_key`)** : `publication_id`
- **Clé de précombine** : `last_updated_timestamp`
- **Partitionnement** : `publication_year`, `university_name`
- **Hive Sync** : désactivé (`enable_hive_sync=False`)

**Exécution** :
```bash
spark-submit transform_publications.py
```

---

### 3️⃣ `index_publications.py` — Indexation dans Elasticsearch

**Rôle** : Lire la table Hudi `research_publications` (couche Silver) et indexer son contenu dans **Elasticsearch** pour permettre une recherche rapide (full-text, filtres, recherche sémantique).

**Étapes** :
1. Initialisation d'une session Spark dédiée (`"Index Publications"`)
2. Lecture de la table Hudi via le connecteur `format("hudi")`
3. Vérification qu'il existe des données à indexer (arrêt anticipé si la table est vide)
4. Création des index Elasticsearch (mappings) via `create_indices()`
5. Indexation en masse (**bulk**) via `ElasticsearchBulkWriter.write_dataframe()`, en utilisant `record_id` comme identifiant de document
6. Arrêt propre de la session Spark

**Exécution** :
```bash
spark-submit index_publications.py
```

**Index cible** : `research_publications`
**Champ ID document Elasticsearch** : `record_id`

---

##  Enchaînement complet du pipeline

```bash
# 1. Extraction depuis OpenAlex (écrit les fichiers bruts JSON dans MinIO Bronze)
python extract_openalex.py

# 2. Transformation Bronze → Silver (Spark + Hudi)
spark-submit transform_publications.py

# 3. Indexation Silver → Elasticsearch
spark-submit index_publications.py
```

---

##  Architecture technique

| Composant | Technologie |
|-----------|-------------|
| Source de données | API [OpenAlex](https://openalex.org) |
| Stockage brut (Bronze) | MinIO (S3-compatible) — format JSON |
| Traitement distribué | Apache Spark (PySpark) |
| Table Lakehouse (Silver) | Apache Hudi |
| Snapshot analytique | Parquet (partitionné) |
| Moteur de recherche | Elasticsearch (indexation bulk) |

## Dépendances principales

- `requests` — appels HTTP vers l'API OpenAlex
- `pyspark` — traitement distribué et écriture Hudi
- Modules internes du projet : `jobs.common` (config, logger, session Spark, écriture Hudi), `jobs.transformation` (lecteurs, helpers de jointure/nettoyage/validation), `jobs.indexing` (mappings et writer Elasticsearch)

##  Points clés de robustesse

- **Extraction résiliente** : pagination par curseur, gestion automatique de la fin de flux
- **Double parsing** : gère les variations de format de réponse OpenAlex (objet direct vs wrapper `results`)
- **Validation systématique** : contrôle de non-nullité et d'unicité avant écriture finale
- **Idempotence via Hudi** : clé d'enregistrement + précombine permettant les mises à jour incrémentales
- **Arrêt anticipé sécurisé** : le job d'indexation s'interrompt proprement si aucune donnée n'est disponible

---

## Contexte du projet

Ce pipeline s'inscrit dans le cadre d'un projet universitaire de **plateforme de données (University Data Platform)**, combinant ingestion multi-API (OpenAlex, Crossref, ORCID), traitement Big Data (Spark), architecture Data Lakehouse (Hudi + Hive Metastore) et recherche/BI (Elasticsearch, Metabase), orchestré via Docker Compose.
