import psycopg2
import pickle

DB_PARAMS = {
    "dbname": "mydb",
    "user": "admin",
    "password": "admin123",
    "host": "postgres",
    "port": 5432
}

COUNTRY = "NER"
PRED_YEARS = [2026, 2027, 2028, 2029, 2030]

conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()

cursor.execute("""
SELECT id, model_name, model_binary
FROM ml_models
WHERE model_type = 'RF' AND country_iso = %s;
""", (COUNTRY,))

models = cursor.fetchall()

for mid, scode, binary in models:
    model = pickle.loads(binary)
    for year in PRED_YEARS:
        pred = float(model.predict([[year]])[0])
        cursor.execute("""
            INSERT INTO ml_predictions (
                model_name, model_type, country_iso, seriescode, year, predicted_value, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (seriescode, year, country_iso)
            DO UPDATE SET
                predicted_value = EXCLUDED.predicted_value,
                model_name = EXCLUDED.model_name,
                model_type = EXCLUDED.model_type,
                created_at = NOW();
        """, (scode, "RF", COUNTRY, scode, year, pred))

conn.commit()
cursor.close()
conn.close()
print("Random Forest predictions inserted/updated successfully.")
