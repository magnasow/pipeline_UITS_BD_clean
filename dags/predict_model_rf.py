import psycopg2
import pickle
import sys
from datetime import datetime


# Paramètres PostgreSQL
DB_PARAMS = {
    "dbname": "mydb",
    "user": "admin",
    "password": "admin123",
    "host": "postgres",
    "port": 5432
}

COUNTRY = "NER"
PRED_YEARS = [2026, 2027, 2028, 2029, 2030]

def main():
    try:
        print("[INFO] Connexion à PostgreSQL...")
        # Connexion DB
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        print("[INFO] Connexion réussie")

        # Récupérer tous les modèles Random Forest du pays
        cursor.execute("""
            SELECT model_name, model_binary
            FROM ml_models
            WHERE model_type = 'RF' AND country_iso = %s;
        """, (COUNTRY,))
        models = cursor.fetchall()
        print(f"[INFO] {len(models)} modèles RF récupérés pour {COUNTRY}")

        if not models:
            print("[WARNING] Aucun modèle RF trouvé. Vérifiez la table ml_models.")
            return

        # Parcours des modèles
        for model_name, binary in models:
            try:
                # Charger le modèle depuis son binaire
                model = pickle.loads(binary)
            except Exception as e:
                print(f"[ERROR] Impossible de charger le modèle {model_name}: {e}")
                continue

            for year in PRED_YEARS:
                try:
                    # Générer des prédictions pour chaque année
                    pred = float(model.predict([[year]])[0])
                    print(f"[INFO] Prédiction {model_name} pour {year}: {pred}")
                    # Stocker ou mettre à jour en base
                    cursor.execute("""
                        INSERT INTO ml_predictions (
                            model_name, model_type, country_iso, seriescode, year, predicted_value, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (seriescode, year, country_iso, model_type)
                        DO UPDATE SET
                            predicted_value = EXCLUDED.predicted_value,
                            model_name = EXCLUDED.model_name,
                            created_at = NOW();
                    """, (model_name, "RF", COUNTRY, model_name, year, pred))
                except Exception as e:
                    print(f"[ERROR] Impossible d'insérer la prédiction {model_name} pour {year}: {e}")
                    continue

        conn.commit()   # Valider les écritures
        print("[INFO] Toutes les prédictions RF ont été sauvegardées avec succès")

    except Exception as e:
        print(f"[FATAL] Erreur générale : {e}")
        sys.exit(1)

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("[INFO] Connexion PostgreSQL fermée")

if __name__ == "__main__":
    main()
