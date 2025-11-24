import psycopg2
import pickle

# Paramètres de connexion à la base
DB_PARAMS = {
    "dbname": "mydb",
    "user": "admin",
    "password": "admin123",
    "host": "postgres",
    "port": 5432
}

COUNTRY = "NER"   # Le pays pour lequel on prédit
PRED_YEARS = [2026, 2027, 2028, 2029, 2030]    # Années futures à prédire

# Connexion à PostgreSQL
conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()

# Récupérer tous les modèles LR
cursor.execute("""
SELECT model_name, model_binary
FROM ml_models
WHERE model_type = 'LR' AND country_iso = %s;
""", (COUNTRY,))

models = cursor.fetchall()
print("Models fetched:", models)  # Vérifie que les modèles existent en base

# Boucle sur chaque modèle récupéré
for model_name, binary in models:
    # Charger le modèle depuis sa version binaire
    model = pickle.loads(binary)
    for year in PRED_YEARS:
        pred = float(model.predict([[year]])[0])   # Prédiction simple

        # Sauvegarder la prédiction dans PostgreSQL
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
        """, (model_name, "LR", COUNTRY, model_name, year, pred))


# Valider les insertions et fermer proprement
conn.commit()
cursor.close()
conn.close()
print("Linear Regression predictions inserted successfully.")
