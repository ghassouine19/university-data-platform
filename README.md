# University Data Platform
## Plateforme Big Data pour la collecte, le traitement et la recherche de données universitaires

---

# Présentation

**University Data Platform** est une plateforme Big Data conçue pour centraliser différentes sources de données produites par une université afin de faciliter leur stockage, leur traitement, leur analyse et leur consultation.

L'objectif est de construire une architecture de type **Data Lakehouse** capable d'automatiser l'ensemble du cycle de vie des données, depuis leur collecte jusqu'à leur exploitation par les utilisateurs.

La plateforme est conçue pour traiter plusieurs types de données :

- Publications scientifiques (OpenAlex)
- Documents (PDF, Word, etc.)
- Images
- Autres sources pouvant être ajoutées ultérieurement

L'architecture a été pensée de manière modulaire afin que chaque source de données puisse disposer de son propre pipeline ETL tout en partageant la même infrastructure Big Data.

---

# Architecture globale du projet

```text
                 Sources de données
        ┌──────────────┬──────────────┬──────────────┐
        │              │              │
        ▼              ▼              ▼
   OpenAlex API     Documents       Images
        │              │              │
        └──────────────┴──────────────┘
                       │
                       ▼
                Apache Airflow
            (Orchestration des pipelines)
                       │
                       ▼
                MinIO (Raw Layer)
                       │
                       ▼
           Apache Spark (ETL distribué)
                       │
                       ▼
              Apache Hudi (Silver)
                       │
                       ▼
             Apache Hive (Catalogue)
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

---

# Notre contribution

Dans le cadre de ce projet, plusieurs pipelines ont été amorcés :

- pipeline des publications scientifiques ;
- pipeline des documents ;
- pipeline des images.

Les phases d'extraction et les premières étapes de transformation ont été initiées pour les documents et les images.

Cependant, le pipeline ayant été entièrement développé, testé et automatisé est celui des **publications scientifiques provenant d'OpenAlex**.

C'est pourquoi un seul DAG Airflow a été implémenté :

```
university_publications_pipeline
```

Ce DAG constitue une démonstration complète du fonctionnement de l'architecture Big Data et pourra servir de modèle pour les futurs pipelines des documents et des images.

---

# Pourquoi un seul DAG ?

Le projet étant conçu de manière modulaire, chaque source de données pourra disposer de son propre DAG.

Dans cette première version, nous avons choisi de finaliser le pipeline OpenAlex car il mobilise l'ensemble des composants de la plateforme :

- Apache Airflow
- Apache Spark
- MinIO
- Apache Hudi
- Apache Hive
- Elasticsearch
- FastAPI
- Streamlit

Les pipelines dédiés aux documents et aux images pourront être ajoutés ultérieurement sans modifier l'architecture globale.

---

# Technologies utilisées

| Technologie | Rôle |
|-------------|------|
| Apache Airflow | Orchestration des pipelines |
| Apache Spark | Traitement distribué des données |
| Apache Hudi | Stockage Lakehouse |
| Apache Hive | Catalogue des tables |
| MinIO | Data Lake |
| Elasticsearch | Recherche plein texte |
| FastAPI | API REST |
| Streamlit | Interface utilisateur |
| Docker | Conteneurisation |
| Python | Développement |

---

# Architecture des données

## Raw Layer

Les données sont stockées dans MinIO sans modification.

```
MinIO
└── raw-json-dev/
```

Pour OpenAlex, deux fichiers sont enregistrés :

```
publications.json
metadata.json
```

Les données originales sont conservées afin de garantir leur traçabilité.

---

## Silver Layer

Les données sont transformées avec Apache Spark puis enregistrées dans Apache Hudi.

Cette couche contient des données :

- nettoyées ;
- normalisées ;
- enrichies ;
- partitionnées.

---

## Apache Hive

Apache Hive ne stocke pas les données.

Son rôle consiste à gérer le catalogue des tables Hudi.

Il conserve :

- le schéma des tables ;
- les partitions ;
- l'emplacement des fichiers dans MinIO.

Grâce à Hive, les données Hudi peuvent être interrogées en SQL.

Exemple :

```sql
SELECT *
FROM silver.research_publications;
```

---

## Elasticsearch

Les données finales sont indexées dans Elasticsearch afin de permettre une recherche rapide par :

- titre ;
- auteur ;
- DOI ;
- journal ;
- année ;
- université.

---

# Le DAG développé

Le pipeline entièrement implémenté est :

```
university_publications_pipeline
```

Il automatise toutes les étapes du traitement des publications scientifiques.

---

# Architecture du DAG

```text
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
 Lecture publications          Lecture metadata
     │                                 │
     └────────────────┬────────────────┘
                      ▼
             Fusion des données
                      │
                      ▼
             Validation Spark
                      │
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

---

# Déroulement détaillé du pipeline

## 1. Extraction

La tâche **extract_openalex** :

- interroge l'API OpenAlex ;
- récupère les publications scientifiques ;
- génère deux fichiers :

```
publications.json
metadata.json
```

- stocke ces fichiers dans MinIO (Raw Layer).

---

## 2. Transformation

Cette étape est réalisée avec Apache Spark.

Elle constitue le cœur du pipeline ETL.

Les traitements sont exécutés dans l'ordre suivant.

### Lecture

Spark lit :

- publications.json
- metadata.json

### Fusion

Les deux fichiers sont fusionnés afin d'associer à chaque publication ses métadonnées d'ingestion.

Cette étape permet d'assurer la traçabilité des données.

### Validation

Spark vérifie :

- le schéma attendu ;
- les types de données ;
- les colonnes obligatoires ;
- la cohérence des données.

### Nettoyage

Les opérations réalisées comprennent notamment :

- suppression des doublons ;
- suppression des colonnes inutiles ;
- traitement des valeurs nulles ;
- conversion des types.

### Normalisation

Les colonnes sont renommées et harmonisées afin d'obtenir une structure unique.

### Enrichissement métier

De nouvelles informations sont calculées ou extraites :

- DOI ;
- titre ;
- auteurs ;
- journal ;
- année ;
- université ;
- identifiants OpenAlex ;
- autres attributs métiers.

### Écriture Hudi

Les données sont ensuite enregistrées dans Apache Hudi afin de bénéficier :

- du versionnement ;
- des mises à jour incrémentales ;
- du partitionnement ;
- d'une meilleure performance de lecture.

### Synchronisation Hive

Hive met automatiquement à jour le catalogue afin de rendre les tables accessibles en SQL.

---

## 3. Indexation

La tâche **index_publications** :

- lit les données Hudi ;
- crée l'index Elasticsearch si nécessaire ;
- transforme chaque publication en document Elasticsearch ;
- indexe les publications.

---

# Flux complet

```text
OpenAlex API
      │
      ▼
Extraction
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
      │
      ▼
Utilisateur
```

---

# Résultat final

À l'issue de l'exécution complète du pipeline :

- les publications sont automatiquement extraites depuis OpenAlex ;
- les données brutes sont stockées dans MinIO ;
- les fichiers `publications.json` et `metadata.json` sont fusionnés ;
- les données sont validées, nettoyées et enrichies avec Apache Spark ;
- les données sont stockées dans Apache Hudi ;
- Hive met à jour le catalogue des tables ;
- Elasticsearch indexe les publications ;
- FastAPI expose les données via une API REST ;
- Streamlit permet aux utilisateurs d'effectuer des recherches multicritères sur les publications scientifiques.

Le pipeline constitue ainsi une chaîne ETL entièrement automatisée démontrant le fonctionnement de l'architecture Big Data de la plateforme et servant de base à l'intégration future des pipelines dédiés aux documents et aux images.