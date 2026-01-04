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
# Chargement du modèle RandomForest
# =============================
cursor.execute("""
SELECT model_object
FROM ml_models
WHERE model_name = 'rf_connectivity'
  AND model_type = 'RandomForest';
""")

row = cursor.fetchone()
if row is None:
    raise ValueError("Modèle Random Forest introuvable en base")

rf_model = pickle.loads(row[0])

# =============================
# Liste des pays
# =============================
countries = pd.read_sql(
    "SELECT DISTINCT entite_iso FROM indicateurs_connectivite_etude",
    conn
)

# =============================
# Prédictions 2026–2030
# =============================
for country in countries["entite_iso"]:

    # Récupération des dernières valeurs connues
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
        print(f"[WARN] Données manquantes pour {country}, ignoré")
        continue

    ambs = df_last.loc[0, "datevaleur_ambs"]
    pcbmnt = df_last.loc[0, "datevaleur_pcbmnt"]

    # Prédictions pour 2026 à 2030
    for year in range(2026, 2031):
        X_future = pd.DataFrame([{
            "dateyear": year,
            "datevaleur_ambs": ambs,
            "datevaleur_pcbmnt": pcbmnt
        }])

        pred = float(rf_model.predict(X_future)[0])

        cursor.execute("""
        INSERT INTO indicateurs_connectivite_predictions
        (entite_iso, dateyear, datevaleur_ambs,
         datevaleur_pcbmnt, prediction_datevaleur_iuti, modele)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (entite_iso, dateyear, modele)
        DO UPDATE SET
            prediction_datevaleur_iuti = EXCLUDED.prediction_datevaleur_iuti,
            created_at = CURRENT_TIMESTAMP;
        """, (country, year, ambs, pcbmnt, pred, "RandomForest"))

# Commit et fermeture de la connexion
conn.commit()
cursor.close()
conn.close()

print("[OK] Prédictions Random Forest 2026–2030 générées")
