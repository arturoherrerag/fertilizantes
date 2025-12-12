import pandas as pd
from sqlalchemy import text
import os
import glob
import unicodedata
import re
from conexion import engine, DB_NAME

# Ruta base de los archivos CSV
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"

# Buscar archivo con la máscara "*-PEDIDOS DESGLOSE-NACIONAL-ANUAL_*"
file_list = glob.glob(os.path.join(ruta_base, "*PEDIDOS DESGLOSE-NACIONAL-ANUAL*"))
if not file_list:
    print("❌ Error: No se encontró ningún archivo que coincida con la máscara.")
    exit()
file_path = file_list[0]

if not os.path.exists(file_path):
    print(f"❌ Error: No se encontró el archivo {file_path}")
    exit()

# Cargar el CSV en un DataFrame
df_pedidos_desglosados = pd.read_csv(file_path, encoding="utf-8", delimiter=",", header=0)

# --------------------------------------------------------------------------
# Función para normalizar nombres de columnas: minúsculas, sin acentos, sin espacios
def normalizar_columna(col):
    col = col.strip()
    col = col.lower()
    col = ''.join(
        c for c in unicodedata.normalize('NFD', col)
        if unicodedata.category(c) != 'Mn'
    )
    col = re.sub(r'[^a-z0-9]+', '_', col)
    col = re.sub(r'[_]+', '_', col).strip('_')
    return col

# Normalizar las columnas del DataFrame
df_pedidos_desglosados.columns = [normalizar_columna(c) for c in df_pedidos_desglosados.columns]

# Reemplazar valores no válidos ("N/A", "NA", "-") con None
df_pedidos_desglosados.replace(["N/A", "NA", "n/a", "-"], None, inplace=True)

# Columnas numéricas que deseas convertir
columnas_numericas = ["dap", "urea"]
for col in columnas_numericas:
    if col in df_pedidos_desglosados.columns:
        df_pedidos_desglosados[col] = pd.to_numeric(df_pedidos_desglosados[col], errors="coerce").round(3)

# 1️⃣ Agregar columna 'id_pedido' con numeración consecutiva
df_pedidos_desglosados.insert(
    0,
    "id_pedido",
    range(1, len(df_pedidos_desglosados) + 1)
)

# 2️⃣ Filtrar solo 'AUTORIZADO'
if "estatus_pedido_detalle" in df_pedidos_desglosados.columns:
    df_pedidos_desglosados = df_pedidos_desglosados.loc[
        df_pedidos_desglosados["estatus_pedido_detalle"] == "AUTORIZADO"
    ]
else:
    print("⚠️ Advertencia: No se encontró la columna 'estatus_pedido_detalle'. No se aplicará el filtro.")

print("✅ Columnas del DataFrame después de limpiar:", df_pedidos_desglosados.columns.tolist())
print("Cantidad de filas con estatus 'AUTORIZADO':", len(df_pedidos_desglosados))

cols_vista = ["id_pedido"] + [c for c in columnas_numericas if c in df_pedidos_desglosados.columns]
print(df_pedidos_desglosados[cols_vista].head(10))

# Importar a PostgreSQL
try:
    with engine.begin() as conn:
        print("🧹 Eliminando registros anteriores de 'pedidos_desglosado'...")
        conn.execute(text("DELETE FROM pedidos_desglosado;"))

        print("⬆️ Insertando nuevos registros...")
        df_pedidos_desglosados.to_sql("pedidos_desglosado", conn, if_exists="append", index=False)

    print(f"✅ Se han sustituido los datos antiguos por los nuevos en la tabla 'pedidos_desglosado' de la base '{DB_NAME}'.")
except Exception as e:
    print(f"❌ Error al importar los datos: {e}")
