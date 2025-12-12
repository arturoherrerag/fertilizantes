import pandas as pd
from sqlalchemy import text
import os
import glob
import unicodedata
import re
from conexion import engine, DB_NAME

# Ruta base de los archivos CSV
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"

# Buscar archivo con la máscara "*SEGUIMIENTO CANTIDADES-NACIONAL-ANUAL_*.csv"
file_list = glob.glob(os.path.join(ruta_base, "*-FERTILIZANTES-PEDIDOS SEGUIMIENTO CANTIDADES MULTIFERTILIZANTES-NACIONAL-ANUAL*.CSV"))
if not file_list:
    print("❌ Error: No se encontró ningún archivo que coincida con la máscara '*-FERTILIZANTES-PEDIDOS SEGUIMIENTO CANTIDADES MULTIFERTILIZANTES-NACIONAL-ANUAL*.CSV'.")
    exit()

file_path = file_list[0]
if not os.path.exists(file_path):
    print(f"❌ Error: No se encontró el archivo {file_path}")
    exit()

print(f"ℹ️ Se utilizará el archivo: {file_path}")

# ──────────────────────────────────────────────────────────
def normalizar_columna(col):
    col = col.strip().lower()
    col = ''.join(
        c for c in unicodedata.normalize('NFD', col)
        if unicodedata.category(c) != 'Mn'
    )
    col = re.sub(r'[^a-z0-9]+', '_', col)
    col = re.sub(r'[_]+', '_', col).strip('_')
    return col

# 1️⃣ Leer el archivo CSV como texto
df_pedidos_sigap = pd.read_csv(file_path, encoding="utf-8", delimiter=",", header=0, dtype=str)

# 2️⃣ Normalizar nombres de columnas
df_pedidos_sigap.columns = [normalizar_columna(c) for c in df_pedidos_sigap.columns]

# 3️⃣ Reemplazar valores no válidos por None
df_pedidos_sigap.replace(["N/A", "NA", "n/a", "-"], None, inplace=True)

# 4️⃣ Convertir a numérico ciertas columnas
columnas_numericas = [
    "dap_solicitado", "urea_solicitada",
    "dap_suministrado", "urea_suministrada",
    "dap_por_suministrar", "urea_por_suministrar",
    "catr_dap", "itr_dap",
    "catr_urea", "itr_urea"
]
for col in columnas_numericas:
    if col in df_pedidos_sigap.columns:
        df_pedidos_sigap[col] = pd.to_numeric(df_pedidos_sigap[col], errors="coerce").round(3)

# 🔍 Verificación rápida
print("✅ Columnas del DataFrame después de limpiar:", df_pedidos_sigap.columns.tolist())
print("🔍 Vista previa de filas:")
print(df_pedidos_sigap.head(5))

# ✅ Filtrar solo columnas que existen en la tabla pedidos_sigap
with engine.connect() as conn:
    result = conn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'pedidos_sigap';"
    ))
    columnas_validas = [row[0] for row in result.fetchall()]
    df_pedidos_sigap = df_pedidos_sigap[[col for col in columnas_validas if col in df_pedidos_sigap.columns]]

# 5️⃣ Insertar en PostgreSQL
try:
    with engine.begin() as conn:
        print("🧹 Eliminando registros anteriores de 'pedidos_sigap'...")
        conn.execute(text("DELETE FROM pedidos_sigap;"))

        print("⬆️ Insertando nuevos registros...")
        df_pedidos_sigap.to_sql("pedidos_sigap", conn, if_exists="append", index=False)

    print(f"✅ Se han sustituido los datos antiguos por los nuevos en la tabla 'pedidos_sigap' de la base '{DB_NAME}'.")
except Exception as e:
    print(f"❌ Error al importar los datos: {e}")
