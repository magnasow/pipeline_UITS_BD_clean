# ---------------------------------------------------------
# Entraînement Linear Regression
# Inputs : dateyear, datevaleur_ambs, datevaleur_pcbmnt
# Target : datevaleur_uiti
# ---------------------------------------------------------

import psycopg2
import pandas as pd
import pickle
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

conn = psycopg2.connect(
    dbname="mydb",
    user="admin",
    password="admin123",
    host="postgres",
    port=5432
)

query = """
SELECT dateyear, datevaleur_ambs, datevaleur_pcbmnt, datevaleur_uiti
FROM indicateurs_connectivite_etude
WHERE datevaleur_ambs IS NOT NULL
  AND datevaleur_pcbmnt IS NOT NULL
  AND datevaleur_uiti IS NOT NULL;
"""
df = pd.read_sql(query, conn)

X = df[["dateyear", "datevaleur_ambs", "datevaleur_pcbmnt"]]
y = df["datevaleur_uiti"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

lr_model = LinearRegression()
lr_model.fit(X_train, y_train)

# Sérialisation
model_binary = pickle.dumps(lr_model)
cursor = conn.cursor()
cursor.execute("""
INSERT INTO ml_models (model_name, model_type, model_object)
VALUES (%s, %s, %s)
ON CONFLICT (model_name)
DO UPDATE SET
    model_object = EXCLUDED.model_object,
    trained_at = CURRENT_TIMESTAMP;
""", ("lr_connectivity", "LinearRegression", psycopg2.Binary(model_binary)))

conn.commit()
cursor.close()
conn.close()
print("[OK] Modèle LR entraîné et sauvegardé")
