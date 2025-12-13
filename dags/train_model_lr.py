# ---------------------------------------------------------
# Importation des librairies nécessaires
# ---------------------------------------------------------

import psycopg2                 # Connexion à PostgreSQL
import pandas as pd            # Manipulation des données
from sklearn.linear_model import LinearRegression       # Modèle LR
from sklearn.preprocessing import OneHotEncoder         
from sklearn.compose import ColumnTransformer           
from sklearn.pipeline import Pipeline                   
import pickle                                            
import sys                                               

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

COUNTRY = "NER"     # Pays étudié pour les prédictions (Niger)

try:
    # -----------------------------------------------------
    # Connexion à la base de données
    # -----------------------------------------------------
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    print("[INFO] Connexion PostgreSQL OK")

    # -----------------------------------------------------
    # Chargement des indicateurs nettoyés depuis PostgreSQL
    # -----------------------------------------------------
    query = """
    SELECT seriescode, seriesid, datayear, datavalue, datasource, region
    FROM indicateurs_nettoyes
    WHERE entityiso = %s
    ORDER BY datayear;
    """

    df = pd.read_sql(query, conn, params=(COUNTRY,))
    print(f"[INFO] Données chargées : {df.shape[0]} lignes, {df['seriescode'].nunique()} indicateurs")

    # Dictionnaire où sera stocké chaque modèle entraîné
    models = {}

    # -----------------------------------------------------
    # Boucle sur chaque indicateur (seriescode)
    # Un modèle LR est entraîné par indicateur
    # -----------------------------------------------------
    for scode in df["seriescode"].unique():

        # Sélection des données associées à cet indicateur
        sub = df[df["seriescode"] == scode].dropna(subset=["datavalue", "datayear"])

        # Si trop peu de données → on ignore
        if len(sub) < 5:
            print(f"[WARNING] Series {scode} ignorée (moins de 5 points)")
            continue

        try:
            # On convertit seriesid en string pour l'encodage OneHotEncoder
            sub["seriesid"] = sub["seriesid"].astype(str)

            # Définition des variables explicatives
            features = ["datayear", "datasource", "region", "seriesid"]
            X = sub[features]

            # Variable cible : la valeur de l'indicateur
            y = sub["datavalue"]

            # -------------------------------------------------
            # Prétraitement : Encoder cat + passer numeric
            # -------------------------------------------------
            preprocessor = ColumnTransformer(
                transformers=[
                    # Encodage one-hot des colonnes catégorielles
                    ("cat", OneHotEncoder(handle_unknown="ignore"),
                     ["datasource", "region", "seriesid"]),

                    # datayear est conservée telle quelle
                    ("num", "passthrough", ["datayear"])
                ]
            )

            # Pipeline complet = prétraitement + modèle
            model = Pipeline([
                ("preprocessing", preprocessor),
                ("regressor", LinearRegression())
            ])

            # Entraînement du modèle
            model.fit(X, y)
            models[scode] = model

            print(f"[INFO] Modèle LR entraîné pour {scode}")

        except Exception as e:
            print(f"[ERROR] Erreur modèle {scode} : {e}")
            continue

    # ---------------------------------------------------------
    # Sauvegarde des modèles dans la base PostgreSQL
    # Chaque modèle est sérialisé avec pickle
    # ---------------------------------------------------------
    for scode, model in models.items():
        try:
            # Sérialisation en binaire
            binary_model = pickle.dumps(model)

            # Insert ou Update dans ml_models
            cursor.execute("""
                INSERT INTO ml_models (model_name, model_type, country_iso, model_binary)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (model_name, model_type, country_iso)
                DO UPDATE SET model_binary = EXCLUDED.model_binary;
            """, (scode, "LR_MULTI", COUNTRY, binary_model))

            print(f"[INFO] Modèle LR {scode} sauvegardé")

        except Exception as e:
            print(f"[ERROR] Sauvegarde DB {scode} : {e}")

    # Validation des insertions
    conn.commit()
    print("[INFO] Tous les modèles LR sauvegardés avec succès")

# ---------------------------------------------------------
# Gestion globale des erreurs
# ---------------------------------------------------------
except Exception as e:
    print(f"[FATAL] Erreur générale : {e}")
    sys.exit(1)

# ---------------------------------------------------------
# Fermeture propre de la connexion PostgreSQL
# ---------------------------------------------------------
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("[INFO] Connexion PostgreSQL fermée")
