import os
import pandas as pd
import glob
import time
import unicodedata
import re
from sqlalchemy import text
from conexion import engine, psycopg_conn, DB_NAME

# ⏱️ Inicio
inicio = time.time()

# 📁 Rutas
base_path = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_padrones"
csv_temp_path = os.path.join(base_path, "CSV_TEMPORAL")
os.makedirs(csv_temp_path, exist_ok=True)

archivo_duplicados = os.path.join(base_path, "duplicados.csv")
archivo_acuses_vacios = os.path.join(base_path, "acuses_estatal_vacios.csv")
csv_temp_carga = "/tmp/derechohabientes_padrones_2025_temp.csv"

# 📋 Columnas esperadas (en orden exacto)
columnas_esperadas = [
    "acuse_estatal",
    "primer_apellido",
    "segundo_apellido",
    "nombre",
    "cultivo",
    "sexo",
    "estado_predio_inegi",
    "municipio_predio_inegi",
    "localidad_predio",
    "id_ceda",
    "dap_ton",
    "urea_ton",
    "superficie_apoyada",
    "publicacion__fecha"
]

# 🧼 Función para limpiar nombres de columnas
def limpiar_columna(col):
    col = unicodedata.normalize('NFKD', col)
    col = ''.join(c for c in col if not unicodedata.combining(c))
    col = col.strip().lower().replace(" ", "_")
    col = re.sub(r"[^a-z0-9_]", "", col)
    return col

# 🔄 Paso 1: Convertir archivos .xlsx a CSV
excel_files = []
for root, dirs, files in os.walk(base_path):
    for file in files:
        if file.endswith(".xlsx") and not file.startswith("~$"):
            excel_files.append(os.path.join(root, file))

csv_generados = []
for archivo in excel_files:
    try:
        df = pd.read_excel(archivo, dtype=str)
        df.columns = [limpiar_columna(c) for c in df.columns]
        nombre_csv = os.path.splitext(os.path.basename(archivo))[0] + ".csv"
        ruta_csv = os.path.join(csv_temp_path, nombre_csv)
        df.to_csv(ruta_csv, index=False, encoding="utf-8")
        csv_generados.append(ruta_csv)
        print(f"✅ Convertido: {archivo}")
    except Exception as e:
        print(f"❌ Error en {archivo}: {e}")

# 📥 Paso 2: Leer todos los .csv generados
df_total = pd.concat(
    (pd.read_csv(archivo, dtype=str, encoding="utf-8") for archivo in csv_generados),
    ignore_index=True
)

# 🧽 Asegurar columnas esperadas y normalizadas
df_total.columns = df_total.columns.str.strip().str.lower()
df_total = df_total[[col for col in columnas_esperadas if col in df_total.columns]]

# 🚨 Identificar registros sin acuse_estatal
df_vacios = df_total[df_total["acuse_estatal"].isna() | (df_total["acuse_estatal"].str.strip() == "")]
if not df_vacios.empty:
    df_vacios.to_csv(archivo_acuses_vacios, index=False, encoding="utf-8")
    print(f"⚠️ {len(df_vacios)} registros eliminados por tener 'acuse_estatal' vacío. Guardados en:\n{archivo_acuses_vacios}")
    df_total = df_total[~df_total.index.isin(df_vacios.index)]

# 🔁 Identificar y eliminar duplicados
df_total["__orden__"] = df_total.index
duplicados = df_total[df_total.duplicated("acuse_estatal", keep=False)].copy()
duplicados.drop(columns="__orden__").to_csv(archivo_duplicados, index=False, encoding="utf-8")
print(f"⚠️ {len(duplicados)} duplicados guardados en {archivo_duplicados}")

df_total = df_total.sort_values("__orden__").drop(columns="__orden__")
df_total = df_total.drop_duplicates("acuse_estatal", keep="last")

# 🧾 Conversión de tipos
df_total["superficie_apoyada"] = pd.to_numeric(df_total["superficie_apoyada"], errors="coerce").fillna(0).astype("Int64")
df_total["publicacion__fecha"] = pd.to_datetime(df_total["publicacion__fecha"], errors="coerce").dt.strftime("%Y-%m-%d")

# 🗃️ Obtener columnas reales de la tabla
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'derechohabientes_padrones_2025'
        ORDER BY ordinal_position
    """))
    columnas_pg = [row[0] for row in result.fetchall()]

# 🧽 Reordenar y asegurar estructura final
df_total = df_total[columnas_pg]

# 💾 Exportar CSV temporal
df_total.to_csv(csv_temp_carga, index=False, header=False, encoding="utf-8", na_rep='')

# 🚀 COPY a PostgreSQL
try:
    with psycopg_conn, psycopg_conn.cursor() as cur:
        print("🗑️ Truncando tabla derechohabientes_padrones_2025...")
        cur.execute("TRUNCATE TABLE derechohabientes_padrones_2025;")
        print("📥 Importando datos con COPY...")
        with open(csv_temp_carga, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"""
                COPY derechohabientes_padrones_2025 ({', '.join(columnas_pg)})
                FROM STDIN WITH CSV
                """, f
            )
    print(f"✅ Se importaron {len(df_total)} registros a 'derechohabientes_padrones_2025'.")
except Exception as e:
    print(f"❌ Error durante la importación con COPY: {e}")

# ⏱️ Fin
fin = time.time()
print(f"⏱️ Tiempo total de ejecución: {round(fin - inicio, 2)} segundos.")