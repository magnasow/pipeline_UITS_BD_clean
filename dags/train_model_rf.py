# ---------------------------------------------------------
# Importation des librairies nécessaires
# ---------------------------------------------------------

import psycopg2                         # Pour se connecter à PostgreSQL
import pandas as pd                    # Pour manipuler les données
from sklearn.ensemble import RandomForestRegressor   # Modèle Random Forest
from sklearn.preprocessing import OneHotEncoder      
from sklearn.compose import ColumnTransformer        
from sklearn.pipeline import Pipeline                
import pickle                                        
import sys                                           

# ---------------------------------------------------------
# Paramètres PostgreSQL pour la connexion
# ---------------------------------------------------------

DB_PARAMS = {
    "dbname": "mydb",
    "user": "admin",
    "password": "admin123",
    "host": "postgres",
    "port": 5432
}

# Pays étudié (Niger)
COUNTRY = "NER"

try:
    # ---------------------------------------------------------
    # Connexion à la base PostgreSQL
    # ---------------------------------------------------------
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    print("[INFO] Connexion PostgreSQL OK")

    # ---------------------------------------------------------
    # Extraction des indicateurs nettoyés depuis PostgreSQL
    # ---------------------------------------------------------
    query = """
    SELECT seriescode, seriesid, datayear, datavalue, datasource, region
    FROM indicateurs_nettoyes
    WHERE entityiso = %s
    ORDER BY datayear;
    """

    df = pd.read_sql(query, conn, params=(COUNTRY,))
    print(f"[INFO] Données chargées : {df.shape[0]} lignes, {df['seriescode'].nunique()} indicateurs")

    # Dictionnaire contenant le modèle pour chaque seriescode
    models = {}

    # ---------------------------------------------------------
    # Boucle d’entraînement : 1 modèle Random Forest par indicateur
    # ---------------------------------------------------------
    for scode in df["seriescode"].unique():

        # Sélection des données propres à l’indicateur
        sub = df[df["seriescode"] == scode].dropna(subset=["datavalue", "datayear"])

        # Vérification : il faut au moins 5 années pour entraîner un modèle
        if len(sub) < 5:
            print(f"[WARNING] Series {scode} ignorée (moins de 5 points)")
            continue

        try:
            # Séries ID converti en string pour OneHotEncoder
            sub["seriesid"] = sub["seriesid"].astype(str)

            # Définition des variables explicatives
            features = ["datayear", "datasource", "region", "seriesid"]
            X = sub[features]

            # Variable cible
            y = sub["datavalue"]

            # ---------------------------------------------------------
            # Prétraitement : OneHotEncoder pour les variables catégorielles
            # ---------------------------------------------------------
            preprocessor = ColumnTransformer(
                transformers=[
                    ("cat", OneHotEncoder(handle_unknown="ignore"),
                     ["datasource", "region", "seriesid"]),
                    ("num", "passthrough", ["datayear"])
                ]
            )

            # ---------------------------------------------------------
            # Pipeline complet Random Forest :
            #   1) Prétraitement
            #   2) RandomForestRegressor
            # ---------------------------------------------------------
            model = Pipeline([
                ("preprocessing", preprocessor),
                ("regressor", RandomForestRegressor(
                    n_estimators=100,       # Nombre d'arbres
                    random_state=42,        # Reproductibilité
                ))
            ])

            # Entraînement du modèle
            model.fit(X, y)

            # Stockage du modèle entraîné
            models[scode] = model

            print(f"[INFO] Modèle RF entraîné pour {scode}")

        except Exception as e:
            print(f"[ERROR] Erreur modèle {scode} : {e}")
            continue

    # ---------------------------------------------------------
    # Sauvegarde des modèles dans PostgreSQL
    # Chaque modèle est sérialisé avec pickle
    # ---------------------------------------------------------
    for scode, model in models.items():
        try:
            # Conversion du modèle en binaire
            binary_model = pickle.dumps(model)

            # Insert ou Update dans la table ml_models
            cursor.execute("""
                INSERT INTO ml_models (model_name, model_type, country_iso, model_binary)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (model_name, model_type, country_iso)
                DO UPDATE SET model_binary = EXCLUDED.model_binary;
            """, (scode, "RF_MULTI", COUNTRY, binary_model))

            print(f"[INFO] Modèle RF {scode} sauvegardé")

        except Exception as e:
            print(f"[ERROR] Sauvegarde DB {scode} : {e}")

    # Validation
    conn.commit()
    print("[INFO] Tous les modèles RF sauvegardés avec succès")

# ---------------------------------------------------------
# Gestion d’erreur globale
# ---------------------------------------------------------
except Exception as e:
    print(f"[FATAL] Erreur générale : {e}")
    sys.exit(1)

# ---------------------------------------------------------
# Fermeture de la connexion PostgreSQL
# ---------------------------------------------------------
finally:
    if 'cursor' in locals(): cursor.close()
    if 'conn' in locals(): conn.close()
    print("[INFO] Connexion PostgreSQL fermée")
