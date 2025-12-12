from sqlalchemy import text
import pandas as pd
from conexion import engine

query = """
SELECT DISTINCT acuse_estatal
FROM full_derechohabientes_2025
WHERE acuse_estatal IS NOT NULL
LIMIT 20
"""

df = pd.read_sql_query(text(query), engine)
print("🗂️ Acuses reales en la base:")
print(df.head(20))