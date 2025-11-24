# Importation des librairies nécessaires
import psycopg2
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import pickle
import sys
# Paramètres de connexion à la base PostgreSQL
DB_PARAMS = {
    "dbname": "mydb",
    "user": "admin",
    "password": "admin123",
    "host": "postgres",
    "port": 5432
}


# Code pays dont on veut entraîner les modèles
COUNTRY = "NER"

try:
    # Connexion à PostgreSQL
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    print("[INFO] Connexion PostgreSQL OK")

    # Requête SQL : sélectionner les données utiles
    query = """
    SELECT seriescode, datayear, datavalue
    FROM indicateurs_nettoyes
    WHERE entityiso = %s
    ORDER BY datayear;
    """
    df = pd.read_sql(query, conn, params=(COUNTRY,))
    print(f"[INFO] Données chargées : {df.shape[0]} lignes, {df['seriescode'].nunique()} series codes")

    # Dictionnaire pour stocker chaque modèle entraîné
    models = {}

    for scode in df["seriescode"].unique():
        sub = df[df["seriescode"] == scode]

        if len(sub) < 3:
            print(f"[WARNING] Series {scode} ignorée (moins de 3 points)")
            continue

        # X = années, reshape pour correspondre au format du modèle
        X = sub["datayear"].values.reshape(-1, 1)
        y = sub["datavalue"].values  # y = valeurs à prédire

        try:
            # Création du modèle Random Forest
            model = RandomForestRegressor(n_estimators=100)

            # Entraînement
            model.fit(X, y)

            # Sauvegarde du modèle dans le dictionnaire
            models[scode] = model
            print(f"[INFO] Modèle RF entraîné pour {scode}")
        except Exception as e:
            print(f"[ERROR] Erreur entraînement RF pour {scode}: {e}")
            continue

    # Sauvegarde de chaque modèle dans la base PostgreSQL
    for scode, model in models.items():
        try:
            # Conversion du modèle en binaire
            binary = pickle.dumps(model)

            # Insertion ou mise à jour dans la table ml_models
            cursor.execute("""
                INSERT INTO ml_models (model_name, model_type, country_iso, model_binary)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (model_name, model_type, country_iso)
                DO UPDATE SET model_binary = EXCLUDED.model_binary;
            """, (scode, "RF", COUNTRY, binary))
            print(f"[INFO] Modèle RF {scode} inséré/mis à jour en DB")
        except Exception as e:
            print(f"[ERROR] Erreur insertion DB pour {scode}: {e}")
            continue

    # Validation des opérations
    conn.commit()
    print("[INFO] Tous les modèles RF sauvegardés avec succès")

except Exception as e:
    print(f"[FATAL] Erreur générale : {e}")
    sys.exit(1)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("[INFO] Connexion PostgreSQL fermée")
