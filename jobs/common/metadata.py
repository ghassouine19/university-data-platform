#Le fichier jobs/common/metadata.py est le garant de la gouvernance des données (Data Governance) et de la qualité
# de l'information (Data Quality) au sein de votre plateforme.
#on a besoin de ce fichier pour
#1-la derive de schema
#2-les corruption des types
#3-la duplication du code
#par exemple
"""La dérive des schémas (Schema Drift) : Si l'API d'une université modifie subitement le nom d'un champ (par exemple, elle remplace id_etudiant par student_id),
 sans contrat clair, votre script Spark va chercher l'ancienne colonne, ne rien trouver, et planter au milieu de la nuit"""