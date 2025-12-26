# ---------------------------------------------------------
# Prédiction 2026–2030 Random Forest
# Inputs : datevaleur_ambs, datevaleur_pcbmnt
# Target : prediction_datevaleur_uiti
# ---------------------------------------------------------

import psycopg2
import pandas as pd
import pickle
import sys

COUNTRY = sys.argv[1] if len(sys.argv) > 1 else "NER"

conn = psycopg2.connect(
    dbname="mydb",
    user="admin",
    password="admin123",
    host="postgres",
    port=5432
)
cursor = conn.cursor()

# Charger le modèle RF
cursor.execute("""
SELECT model_object FROM ml_models
WHERE model_name = 'rf_connectivity';
""")
rf_model = pickle.loads(cursor.fetchone()[0])

# Dernières valeurs connues pour le pays
query = """
SELECT datevaleur_ambs, datevaleur_pcbmnt
FROM indicateurs_connectivite_etude
WHERE entite_iso = %s
  AND datevaleur_ambs IS NOT NULL
  AND datevaleur_pcbmnt IS NOT NULL
ORDER BY dateyear DESC
LIMIT 1;
"""
df_last = pd.read_sql(query, conn, params=(COUNTRY,))
if df_last.empty:
    raise ValueError(f"Aucune donnée pour le pays {COUNTRY}")

ambs = df_last.loc[0, "datevaleur_ambs"]
pcbmnt = df_last.loc[0, "datevaleur_pcbmnt"]

years = list(range(2026, 2031))
X_future = pd.DataFrame({
    "dateyear": years,
    "datevaleur_ambs": [ambs]*len(years),
    "datevaleur_pcbmnt": [pcbmnt]*len(years)
})

predictions = rf_model.predict(X_future)
X_future["prediction_datevaleur_uiti"] = predictions
X_future["entite_iso"] = COUNTRY
X_future["modele"] = "RandomForest"

# Insertion dans la table predictions
for _, row in X_future.iterrows():
    cursor.execute("""
    INSERT INTO indicateurs_connectivite_predictions
    (entite_iso, dateyear, datevaleur_ambs, datevaleur_pcbmnt, prediction_datevaleur_uiti, modele)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (entite_iso, dateyear, modele)
    DO UPDATE SET
        datevaleur_ambs = EXCLUDED.datevaleur_ambs,
        datevaleur_pcbmnt = EXCLUDED.datevaleur_pcbmnt,
        prediction_datevaleur_uiti = EXCLUDED.prediction_datevaleur_uiti,
        created_at = CURRENT_TIMESTAMP;
    """, (row.entite_iso, row.dateyear, row.datevaleur_ambs, row.datevaleur_pcbmnt, row.prediction_datevaleur_uiti, row.modele))

conn.commit()
cursor.close()
conn.close()
print("[OK] Prédictions RF insérées pour 2026–2030")
