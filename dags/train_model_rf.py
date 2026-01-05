import psycopg2
import pickle
import pandas as pd

from sklearn.multioutput import MultiOutputRegressor
from sklearn.ensemble import RandomForestRegressor


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

# =============================
# Chargement des données propres (pour le Niger - Test)
# =============================

query = """
SELECT dateyear,
       datevaleur_ambs,
       datevaleur_pcbmnt,
       datevaleur_iuti
FROM indicateurs_connectivite_etude
WHERE datevaleur_ambs IS NOT NULL
  AND datevaleur_pcbmnt IS NOT NULL
  AND datevaleur_iuti IS NOT NULL
  AND entite_iso = 'NER';
  """
df = pd.read_sql(query, conn)

# =============================
# Préparation des données
# =============================

# 2. Build set like (t -> t+1)
X = df[['datevaleur_ambs', 'datevaleur_pcbmnt', 'datevaleur_iuti']].iloc[:-1].values
y = df[['datevaleur_ambs', 'datevaleur_pcbmnt', 'datevaleur_iuti']].iloc[1:].values

# =============================
# Entraînement Random Forest
# =============================
model = MultiOutputRegressor(
    RandomForestRegressor(
        n_estimators=200,
        random_state=42
    )
)

model.fit(X, y)

# =============================
# Sérialisation du modèle
# =============================
model_binary = pickle.dumps(model)

# =============================
# Sauvegarde en base
# =============================
cursor = conn.cursor()
cursor.execute("""
INSERT INTO ml_models (model_name, model_type, model_object)
VALUES (%s, %s, %s)
ON CONFLICT (model_name, model_type)
DO UPDATE SET
    model_object = EXCLUDED.model_object,
    trained_at = CURRENT_TIMESTAMP;
""", ("rf_connectivity", "RandomForest", psycopg2.Binary(model_binary)))

conn.commit()
cursor.close()
conn.close()

print("[OK] Modèle Random Forest entraîné et sauvegardé")
