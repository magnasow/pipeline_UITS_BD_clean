import psycopg2
import pandas as pd
import pickle
import numpy as np  # <- nécessaire pour reshape

# =============================
# Connexion PostgreSQL
# =============================
conn = psycopg2.connect(
    dbname="mydb",
    user="admin",
    password="admin123",
    host="postgres",
    port=5432
)
cursor = conn.cursor()

# =============================
# Chargement du modèle LR
# =============================
cursor.execute("""
SELECT model_object
FROM ml_models
WHERE model_name='lr_connectivity'
  AND model_type='LinearRegression';
""")
lr_model = pickle.loads(cursor.fetchone()[0])

# =============================
# Liste des pays (choix juste du Niger pour test)
# =============================
countries = pd.read_sql(
    "SELECT DISTINCT entite_iso FROM indicateurs_connectivite_etude WHERE entite_iso = 'NER';",
    conn
)

# =============================
# Prédictions 2026–2030
# =============================
for country in countries["entite_iso"]:

    df_last = pd.read_sql("""
        SELECT datevaleur_ambs, datevaleur_pcbmnt, datevaleur_iuti
        FROM indicateurs_connectivite_etude
        WHERE entite_iso = %s
          AND datevaleur_ambs IS NOT NULL
          AND datevaleur_pcbmnt IS NOT NULL
          AND datevaleur_iuti IS NOT NULL
        ORDER BY dateyear DESC
        LIMIT 1;
    """, conn, params=(country,))

    if df_last.empty:
        continue

    last_values = df_last.iloc[-1].values

    # reshape pour 2D
    pred = np.array(last_values).reshape(1, -1)

    for year in range(2026, 2031):
        pred = lr_model.predict(pred)

        pred_to_insert = np.array(pred).reshape(-1)

        cursor.execute("""
        INSERT INTO indicateurs_connectivite_predictions
        (entite_iso, dateyear, datevaleur_ambs,
         datevaleur_pcbmnt, prediction_datevaleur_iuti, modele)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (entite_iso, dateyear, modele)
        DO UPDATE SET
            prediction_datevaleur_iuti = EXCLUDED.prediction_datevaleur_iuti,
            created_at = CURRENT_TIMESTAMP;
        """, (country, year, pred_to_insert[0], pred_to_insert[1], pred_to_insert[2], "LinearRegression"))

conn.commit()
cursor.close()
conn.close()

print("[OK] Prédictions Linear Regression 2026–2030 générées")
