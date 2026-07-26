# University Publications Pipeline -- Airflow DAG

## Présentation

Le DAG **`university_publications_pipeline`** orchestre automatiquement
le pipeline ETL permettant de collecter, transformer, stocker et indexer
les publications scientifiques d'une université.

Le pipeline s'appuie sur **Apache Airflow**, **Apache Spark**,
**MinIO**, **Apache Hudi**, **Apache Hive**, **Elasticsearch**,
**FastAPI** et **Streamlit**.

------------------------------------------------------------------------

# Architecture générale

``` text
                 OpenAlex API
                      │
                      ▼
             extract_openalex
                      │
                      ▼
          MinIO (Raw JSON Files)
        ┌──────────────────────────┐
        │ publications.json        │
        │ metadata.json            │
        └──────────────────────────┘
                      │
                      ▼
        transform_publications
          (Apache Spark ETL)
                      │
     ┌────────────────┴────────────────┐
     │                                 │
     ▼                                 ▼
 Validation                 Fusion publications
 (schéma Spark)          + metadata.json
     │                                 │
     └────────────────┬────────────────┘
                      ▼
             Nettoyage des données
                      │
                      ▼
          Normalisation des colonnes
                      │
                      ▼
          Enrichissement métier
                      │
                      ▼
          Écriture Apache Hudi
                      │
                      ▼
          Synchronisation Hive
                      │
                      ▼
          index_publications
                      │
                      ▼
             Elasticsearch
                      │
                      ▼
           FastAPI + Streamlit
```

------------------------------------------------------------------------

# Déroulement du pipeline

## 1. Extraction

La tâche **extract_openalex** :

-   interroge l'API OpenAlex ;
-   récupère les publications ;
-   génère deux fichiers :
    -   `publications.json`
    -   `metadata.json`
-   stocke ces fichiers dans MinIO (Raw).

------------------------------------------------------------------------

## 2. Transformation

La tâche **transform_publications** réalise successivement :

1.  Lecture de `publications.json`.
2.  Lecture de `metadata.json`.
3.  Fusion des publications avec leurs métadonnées d'ingestion.
4.  Validation du schéma Spark et des données.
5.  Nettoyage (doublons, valeurs inutiles, types).
6.  Normalisation des colonnes.
7.  Enrichissement métier (DOI, auteurs, année, université, etc.).
8.  Écriture des données au format Apache Hudi.
9.  Synchronisation des métadonnées avec Apache Hive.

### Pourquoi Hive ?

Apache Hive ne stocke pas les données.

Son rôle est de maintenir le catalogue des tables Hudi :

-   schéma des colonnes ;
-   partitions ;
-   emplacement des fichiers dans MinIO.

Grâce à Hive, les tables Hudi deviennent interrogeables en SQL.

``` sql
SELECT *
FROM silver.research_publications;
```

------------------------------------------------------------------------

## 3. Indexation

La tâche **index_publications** :

-   lit les données Hudi ;
-   crée l'index Elasticsearch si nécessaire ;
-   transforme chaque publication en document ;
-   indexe les données pour la recherche plein texte.

------------------------------------------------------------------------

# Flux complet

``` text
OpenAlex API
      │
      ▼
publications.json
metadata.json
      │
      ▼
Fusion
      │
      ▼
Validation
      │
      ▼
Nettoyage
      │
      ▼
Normalisation
      │
      ▼
Enrichissement
      │
      ▼
Apache Hudi
      │
      ▼
Apache Hive
      │
      ▼
Elasticsearch
      │
      ▼
FastAPI
      │
      ▼
Streamlit
```

------------------------------------------------------------------------

# Technologies

-   Apache Airflow
-   Apache Spark
-   MinIO
-   Apache Hudi
-   Apache Hive
-   Elasticsearch
-   FastAPI
-   Streamlit
-   Docker
-   Python

------------------------------------------------------------------------

# Exécution

``` bash
airflow tasks test university_publications_pipeline extract_openalex 2026-07-25

airflow tasks test university_publications_pipeline transform_publications 2026-07-25

airflow tasks test university_publications_pipeline index_publications 2026-07-25
```
