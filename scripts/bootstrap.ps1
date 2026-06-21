# scripts/bootstrap.ps1
$ErrorActionPreference = "Stop"

Write-Host "=================================================================" -ForegroundColor Green
Write-Host "University Data Platform - Script d'amorçage (Windows PowerShell)" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green

# 1. Création de l'arborescence complète des répertoires (si manquants)
Write-Host "Création des répertoires pour l'infrastructure..." -ForegroundColor Cyan
$JarDir = ".\docker\spark\jars"
$AirflowDir = ".\docker\airflow"

New-Item -ItemType Directory -Force -Path "$JarDir\hadoop-aws" | Out-Null
New-Item -ItemType Directory -Force -Path "$JarDir\aws-sdk" | Out-Null
New-Item -ItemType Directory -Force -Path "$JarDir\hudi" | Out-Null
New-Item -ItemType Directory -Force -Path "$JarDir\elasticsearch" | Out-Null
New-Item -ItemType Directory -Force -Path $AirflowDir | Out-Null

# 2. Téléchargement des dépendances Java (JARs) et de l'archive Spark (.tgz)
Write-Host "Téléchargement des binaires lourds de la plateforme..." -ForegroundColor Cyan

# A. Connecteur Hadoop AWS (3.3.4)
$HadoopPath = "$JarDir\hadoop-aws\hadoop-aws.jar"
if (-not (Test-Path $HadoopPath)) {
    Write-Host "-> Téléchargement de hadoop-aws.jar..." -ForegroundColor Gray
    Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -OutFile $HadoopPath
}

# B. Apache Spark tgz pour Airflow (3.5.2)
$SparkTgzPath = "$AirflowDir\spark-3.5.2-bin-hadoop3.tgz"
if (-not (Test-Path $SparkTgzPath)) {
    Write-Host "-> Téléchargement de spark-3.5.2-bin-hadoop3.tgz..." -ForegroundColor Gray
    Invoke-WebRequest -Uri "https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz" -OutFile $SparkTgzPath
}

# C. Connecteur Elasticsearch Spark (8.12.1)
$ElasticPath = "$JarDir\elasticsearch\elasticsearch-spark-30_2.12.jar"
if (-not (Test-Path $ElasticPath)) {
    Write-Host "-> Téléchargement de elasticsearch-spark-30_2.12.jar..." -ForegroundColor Gray
    Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.12.1/elasticsearch-spark-30_2.12-8.12.1.jar" -OutFile $ElasticPath
}

# D. Apache Hudi Spark 3.5 Bundle (0.15.0)
$HudiPath = "$JarDir\hudi\hudi-spark3.5-bundle.jar"
if (-not (Test-Path $HudiPath)) {
    Write-Host "-> Téléchargement de hudi-spark3.5-bundle.jar..." -ForegroundColor Gray
    Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/0.15.0/hudi-spark3.5-bundle_2.12-0.15.0.jar" -OutFile $HudiPath
}

# E. AWS Java SDK Bundle (1.12.262)
$AwsSdkPath = "$JarDir\aws-sdk\aws-java-sdk-bundle.jar"
if (-not (Test-Path $AwsSdkPath)) {
    Write-Host "-> Téléchargement de aws-java-sdk-bundle.jar..." -ForegroundColor Gray
    Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -OutFile $AwsSdkPath
}

# F. Connecteur JDBC PostgreSQL (42.6.0)
$PostgresJarPath = "$JarDir\optional\postgresql.jar"
if (-not (Test-Path $PostgresJarPath)) {
    Write-Host "-> Téléchargement de postgresql.jar..." -ForegroundColor Gray
    Invoke-WebRequest -Uri "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" -OutFile $PostgresJarPath
}

Write-Host "=================================================================" -ForegroundColor Green
Write-Host "   Amorçage de l'infrastructure terminé avec succès !" -ForegroundColor Green
Write-Host "   Exécutez la commande suivante pour compiler et tout lancer :" -ForegroundColor Green
Write-Host "   docker compose --env-file .env.dev -f docker-compose.dev.yml up --build -d" -ForegroundColor Yellow
Write-Host "=================================================================" -ForegroundColor Green