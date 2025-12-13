# ---------------------------------------------------------
# Importation des librairies nécessaires
# ---------------------------------------------------------

import psycopg2                 # Connexion à PostgreSQL
import pickle                   # Chargement des modèles sauvegardés
import pandas as pd             # Manipulation des données tabulaires

# ---------------------------------------------------------
# Paramètres de connexion à PostgreSQL
# ---------------------------------------------------------

DB_PARAMS = {
    "dbname": "mydb",
    "user": "admin",
    "password": "admin123",
    "host": "postgres",
    "port": 5432
}

COUNTRY = "NER"  # Pays étudié pour les prédictions
PRED_YEARS = [2026, 2027, 2028, 2029, 2030]  # Années à prédire

# ---------------------------------------------------------
# Connexion à la base de données
# ---------------------------------------------------------
conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()
print("[INFO] Connexion PostgreSQL OK")

# ---------------------------------------------------------
# Chargement des modèles Random Forest (RF_MULTI)
# Ces modèles ont préalablement été entraînés et stockés
# dans la table ml_models.
# ---------------------------------------------------------
cursor.execute("""
SELECT model_name, model_binary
FROM ml_models
WHERE model_type = 'RF_MULTI' AND country_iso = %s;
""", (COUNTRY,))

models = cursor.fetchall()
print(f"[INFO] {len(models)} modèles RF chargés")

# ---------------------------------------------------------
# Chargement des métadonnées nécessaires pour la prédiction
# (datasource, région, seriesid)
# ---------------------------------------------------------
cursor.execute("""
SELECT DISTINCT seriescode, datasource, region, seriesid
FROM indicateurs_nettoyes
WHERE entityiso = %s;
""", (COUNTRY,))

meta = pd.DataFrame(
    cursor.fetchall(),
    columns=["seriescode", "datasource", "region", "seriesid"]
)
print("[INFO] Métadonnées récupérées")

# ---------------------------------------------------------
# Génération des prédictions pour toutes les séries
# ---------------------------------------------------------
for model_name, binary in models:

    # Réactivation du modèle ML à partir du binaire stocké
    model = pickle.loads(binary)

    # Récupération des métadonnées associées à cet indicateur
    info = meta[meta["seriescode"] == model_name]

    # Si aucune info → on ignore
    if info.empty:
        print(f"[WARNING] Pas de métadonnées pour {model_name}, ignoré.")
        continue

    datasource = info.iloc[0]["datasource"]
    region = info.iloc[0]["region"]
    seriesid = str(info.iloc[0]["seriesid"])  # Conversion en string pour compatibilité

    # -----------------------------------------------------
    # Prédiction sur toutes les années futures définies
    # -----------------------------------------------------
    for year in PRED_YEARS:

        # Création d’une ligne d’entrée conforme au modèle RF
        input_row = pd.DataFrame([{
            "datayear": year,
            "datasource": datasource,
            "region": region,
            "seriesid": seriesid
        }])

        # Génération de la prédiction
        pred = float(model.predict(input_row)[0])

        # Insertion ou mise à jour de la prédiction dans PostgreSQL
        cursor.execute("""
        INSERT INTO ml_predictions (
            model_name, model_type, country_iso,
            seriescode, year, predicted_value, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (seriescode, year, country_iso, model_type)
        DO UPDATE SET predicted_value = EXCLUDED.predicted_value,
                      created_at = NOW();
        """, (model_name, "RF_MULTI", COUNTRY, model_name, year, pred))

        print(f"[OK] {model_name} → {year} = {pred}")

# ---------------------------------------------------------
# Validation et fermeture de la connexion SQL
# ---------------------------------------------------------
conn.commit()
cursor.close()
conn.close()
print("[INFO] Prédictions RF insérées avec succès")
