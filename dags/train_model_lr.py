import psycopg2
import pandas as pd
import pickle
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

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
# Chargement des données propres
# =============================

query = """
SELECT dateyear,
       datevaleur_ambs,
       datevaleur_pcbmnt,
       datevaleur_iuti
FROM indicateurs_connectivite_etude
WHERE datevaleur_ambs IS NOT NULL
  AND datevaleur_pcbmnt IS NOT NULL
  AND datevaleur_iuti IS NOT NULL;
"""

df = pd.read_sql(query, conn)

# Variables explicatives (X) et cible (y)
X = df[["dateyear", "datevaleur_ambs", "datevaleur_pcbmnt"]]
y = df["datevaleur_iuti"]

# Séparation train/test (test ignoré ici)
X_train, _, y_train, _ = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# =============================
# Entraînement Linear Regression
# =============================

model = LinearRegression()
model.fit(X_train, y_train)

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
