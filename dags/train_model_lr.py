import psycopg2
import pandas as pd
from sklearn.linear_model import LinearRegression
import pickle
import sys
from datetime import datetime

DB_PARAMS = {
    "dbname": "mydb",
    "user": "admin",
    "password": "admin123",
    "host": "postgres",
    "port": 5432
}

COUNTRY = "NER"

try:
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    print("[INFO] Connexion PostgreSQL OK")

    # Charger les données
    query = """
    SELECT seriescode, datayear, datavalue
    FROM indicateurs_nettoyes
    WHERE entityiso = %s
    ORDER BY datayear;
    """
    df = pd.read_sql(query, conn, params=(COUNTRY,))
    print(f"[INFO] Données chargées : {df.shape[0]} lignes, {df['seriescode'].nunique()} series codes")

    models = {}

    for scode in df["seriescode"].unique():
        sub = df[df["seriescode"] == scode]

        if len(sub) < 3:
            print(f"[WARNING] Series {scode} ignorée (moins de 3 points)")
            continue

        X = sub["datayear"].values.reshape(-1, 1)
        y = sub["datavalue"].values

        try:
            model = LinearRegression()
            model.fit(X, y)
            models[scode] = model
            print(f"[INFO] Modèle LR entraîné pour {scode}")
        except Exception as e:
            print(f"[ERROR] Erreur entraînement LR pour {scode}: {e}")
            continue

    # Sauvegarder les modèles dans PostgreSQL
    for scode, model in models.items():
        try:
            binary = pickle.dumps(model)
            cursor.execute("""
                INSERT INTO ml_models (model_name, model_type, country_iso, model_binary)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (model_name, model_type, country_iso)
                DO UPDATE SET model_binary = EXCLUDED.model_binary;
            """, (scode, "LR", COUNTRY, binary))
            print(f"[INFO] Modèle LR {scode} inséré/mis à jour en DB")
        except Exception as e:
            print(f"[ERROR] Erreur insertion DB pour {scode}: {e}")
            continue

    conn.commit()
    print("[INFO] Tous les modèles LR sauvegardés avec succès")

except Exception as e:
    print(f"[FATAL] Erreur générale : {e}")
    sys.exit(1)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("[INFO] Connexion PostgreSQL fermée")
