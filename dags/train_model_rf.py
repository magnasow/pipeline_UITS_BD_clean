import psycopg2
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
PRED_YEARS = [2026, 2027, 2028, 2029, 2030]

try:
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    print("[INFO] Connexion PostgreSQL OK")

    # Charger les modèles RF
    cursor.execute("""
        SELECT id, model_name, model_binary
        FROM ml_models
        WHERE model_type = 'RF' AND country_iso = %s;
    """, (COUNTRY,))
    models = cursor.fetchall()
    print(f"[INFO] {len(models)} modèles RF récupérés pour {COUNTRY}")

    for mid, scode, binary in models:
        try:
            model = pickle.loads(binary)
        except Exception as e:
            print(f"[ERROR] Impossible de charger le modèle {scode}: {e}")
            continue

        for year in PRED_YEARS:
            try:
                pred = float(model.predict([[year]])[0])
                cursor.execute("""
                    INSERT INTO ml_predictions (model_name, model_type, country_iso, seriescode, year, predicted_value, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (seriescode, year, country_iso)
                    DO UPDATE SET predicted_value = EXCLUDED.predicted_value,
                                  model_name = EXCLUDED.model_name,
                                  model_type = EXCLUDED.model_type,
                                  created_at = NOW();
                """, (scode, "RF", COUNTRY, scode, year, pred))
                print(f"[INFO] Prédiction {scode} pour {year} insérée/mise à jour: {pred}")
            except Exception as e:
                print(f"[ERROR] Impossible d'insérer prédiction {scode} pour {year}: {e}")
                continue

    conn.commit()
    print("[INFO] Toutes les prédictions sauvegardées avec succès")

except Exception as e:
    print(f"[FATAL] Erreur générale : {e}")
    sys.exit(1)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("[INFO] Connexion PostgreSQL fermée")
