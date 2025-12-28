import psycopg2
import pandas as pd
import pickle

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
WHERE model_name = 'lr_connectivity'
  AND model_type = 'LinearRegression';
""")

row = cursor.fetchone()
if row is None:
    raise ValueError("Modèle Linear Regression introuvable en base")

lr_model = pickle.loads(row[0])

# =============================
# Liste des pays
# =============================
countries = pd.read_sql(
    "SELECT DISTINCT entite_iso FROM indicateurs_connectivite_etude",
    conn
)

# =============================
# Prédictions
# =============================
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

    # Sécurité supplémentaire
    if df_last.empty:
        print(f"[WARN] Données manquantes pour {country}, ignoré")
        continue

    ambs = df_last.loc[0, "datevaleur_ambs"]
    pcbmnt = df_last.loc[0, "datevaleur_pcbmnt"]

    # =============================
    # Prédiction 2026–2030
    # =============================
    for year in range(2026, 2031):

        X_future = pd.DataFrame([{
            "dateyear": year,
            "datevaleur_ambs": ambs,
            "datevaleur_pcbmnt": pcbmnt
        }])

        pred = float(lr_model.predict(X_future)[0])

        cursor.execute("""
        INSERT INTO indicateurs_connectivite_predictions
        (entite_iso, dateyear, datevaleur_ambs,
         datevaleur_pcbmnt, prediction_datevaleur_iuti, modele)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (entite_iso, dateyear, modele)
        DO UPDATE SET
            prediction_datevaleur_iuti = EXCLUDED.prediction_datevaleur_iuti,
            created_at = CURRENT_TIMESTAMP;
        """, (country, year, ambs, pcbmnt, pred, "LinearRegression"))

conn.commit()
cursor.close()
conn.close()

print("[OK] Prédictions Linear Regression générées")
