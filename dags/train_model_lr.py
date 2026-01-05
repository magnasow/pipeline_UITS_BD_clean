import psycopg2
import pandas as pd
import pickle

from sklearn.multioutput import MultiOutputRegressor
from sklearn.linear_model import LinearRegression

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
# Entraînement du MultiOutputRegressor Linear Regression
# =============================
# 3. Train multivariate model
model = MultiOutputRegressor(LinearRegression())
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
""", ("lr_connectivity", "LinearRegression", psycopg2.Binary(model_binary)))

conn.commit()
cursor.close()
conn.close()

print("[OK] Modèle Linear Regression entraîné et sauvegardé")
