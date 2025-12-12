import pandas as pd
from sqlalchemy import text
import os
import glob
import unicodedata
import re
from dateutil import parser
from conexion import engine, DB_NAME

# Ruta base de los archivos CSV
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"

# Buscar archivo con la máscara "*-INCIDENTES-NACIONAL-ANUAL_*"
file_list = glob.glob(os.path.join(ruta_base, "*INCIDENTES-NACIONAL-ANUAL_*"))
if not file_list:
    print("❌ No se encontró ningún archivo con la máscara '*INCIDENTES-NACIONAL-ANUAL*'")
    exit()

file_path = file_list[0]
print(f"ℹ️ Se utilizará el archivo: {file_path}")

# -----------------------------------------------------------------------------
# Función para normalizar nombres de columnas
# -----------------------------------------------------------------------------
def normalizar_columna(col):
    col = col.strip().lower()
    col = ''.join(
        c for c in unicodedata.normalize('NFD', col)
        if unicodedata.category(c) != 'Mn'
    )
    col = re.sub(r'[^a-z0-9]+', '_', col)
    col = re.sub(r'[_]+', '_', col).strip('_')
    return col

# 1️⃣ Leer CSV como cadenas
df_incidencias = pd.read_csv(file_path, dtype=str, encoding='utf-8', delimiter=',')

# 2️⃣ Normalizar nombres de columnas
df_incidencias.columns = [normalizar_columna(c) for c in df_incidencias.columns]

# 3️⃣ Reemplazar valores inválidos con None
df_incidencias.replace(["N/A", "NA", "n/a", "-"], None, inplace=True)

# 4️⃣ Convertir a numérico la columna 'ton_incidencia' (si existe)
if "ton_incidencia" in df_incidencias.columns:
    df_incidencias["ton_incidencia"] = pd.to_numeric(
        df_incidencias["ton_incidencia"], 
        errors="coerce"
    ).round(3)

# 5️⃣ Convertir 'fecha_incidente' a formato fecha
if "fecha_incidente" in df_incidencias.columns:
    def parse_fecha(valor):
        if pd.notna(valor):
            return parser.parse(valor, dayfirst=True).date()
        return None
    df_incidencias["fecha_incidente"] = df_incidencias["fecha_incidente"].apply(parse_fecha)

# 6️⃣ Vista previa
print("\n🎯 Vista previa del DataFrame:")
print(df_incidencias.head(10))

# 7️⃣ Importar datos
try:
    with engine.begin() as conn:
        print("🧹 Eliminando registros anteriores de la tabla 'incidentes'...")
        conn.execute(text("DELETE FROM incidentes;"))

        print("⬆️ Insertando nuevos registros...")
        df_incidencias.to_sql("incidentes", conn, if_exists="append", index=False)

    print(f"✅ Se han sustituido los datos antiguos por los nuevos en la tabla 'incidentes' de la base '{DB_NAME}'.")
except Exception as e:
    print(f"❌ Error al importar los datos: {e}")
