import psycopg2
import pandas as pd
import pickle

conn = psycopg2.connect(
    dbname="mydb",
    user="admin",
    password="admin123",
    host="postgres",
    port=5432
)

cursor = conn.cursor()

cursor.execute("""
SELECT model_object
FROM ml_models
WHERE model_name='lr_connectivity'
  AND model_type='LinearRegression';
""")

lr_model = pickle.loads(cursor.fetchone()[0])

countries = pd.read_sql(
    "SELECT DISTINCT entite_iso FROM indicateurs_connectivite_etude",
    conn
)

for country in countries["entite_iso"]:

    df_last = pd.read_sql("""
        SELECT datevaleur_ambs, datevaleur_pcbmnt
        FROM indicateurs_connectivite_etude
        WHERE entite_iso = %s
          AND datevaleur_ambs IS NOT NULL
          AND datevaleur_pcbmnt IS NOT NULL
        ORDER BY dateyear DESC
        LIMIT 1;
    """, conn, params=(country,))

    if df_last.empty:
        continue

    ambs = df_last.loc[0, "datevaleur_ambs"]
    pcbmnt = df_last.loc[0, "datevaleur_pcbmnt"]

    for year in range(2026, 2031):
        pred = lr_model.predict([[year, ambs, pcbmnt]])[0]

        cursor.execute("""
        INSERT INTO indicateurs_connectivite_predictions
        (entite_iso, dateyear, datevaleur_ambs,
         datevaleur_pcbmnt, prediction_datevaleur_iuti, modele)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (entite_iso, dateyear, modele)
        DO UPDATE SET
            prediction_datevaleur_iuti = EXCLUDED.prediction_datevaleur_iuti,
            created_at = CURRENT_TIMESTAMP;
        """, (country, year, ambs, pcbmnt, float(pred), "LinearRegression"))

conn.commit()
cursor.close()
conn.close()

print("[OK] Prédictions Linear Regression 2026–2030 générées")
