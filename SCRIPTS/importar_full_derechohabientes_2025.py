import os
import pandas as pd
import glob
import time
from sqlalchemy import text
from conexion import engine, psycopg_conn, DB_NAME

# ⏱️ Medir tiempo de ejecución
inicio = time.time()

# 📂 Ruta con archivos CSV
carpeta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/full_derechohabientes/"
archivos_csv = glob.glob(os.path.join(carpeta_csv, "*.CSV"))

# 📊 Combinar todos los archivos en un solo DataFrame
df_full = pd.concat(
    (pd.read_csv(archivo, encoding="utf-8", dtype=str) for archivo in archivos_csv),
    ignore_index=True
)

# 🛠️ Paso 1: Renombrar columnas específicas ANTES de normalizar

# 🔄 Solo renombrar si la columna existe exactamente como "Estatus Solicitud"
if "Estatus Solicitud" in df_full.columns:
    df_full = df_full.rename(columns={"Estatus Solicitud": "estatus_solicitud_pago"})

# 🔄 También aplicar renombramientos seguros para columnas con mayúsculas iniciales
renombramientos_especiales = {
    "dap_toneladas": "dap_entregada",
    "urea_toneladas": "urea_entregada"
}

# Buscar nombres que coincidan ignorando mayúsculas y espacios
nuevos_nombres = {}
for col in df_full.columns:
    col_limpio = col.strip().lower().replace(" ", "_")
    if col_limpio in renombramientos_especiales:
        nuevos_nombres[col] = renombramientos_especiales[col_limpio]

df_full = df_full.rename(columns=nuevos_nombres)

# 🧼 Paso 2: Normalizar todos los nombres
df_full.columns = (
    pd.Series(df_full.columns)
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("á", "a")
    .str.replace("é", "e")
    .str.replace("í", "i")
    .str.replace("ó", "o")
    .str.replace("ú", "u")
    .str.replace("ñ", "n")
    .str.replace(r"[^a-z0-9_]", "", regex=True)
)

# 🧪 Paso 2.5: Duplicados por acuse_estatal
if "acuse_estatal" in df_full.columns:
    df_full["__orden_original__"] = df_full.index
    duplicados = df_full[df_full.duplicated("acuse_estatal", keep="first")].copy()

    ruta_duplicados = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/U_TEMPORAL/duplicados_full_derechohabientes.csv"
    duplicados.to_csv(ruta_duplicados, index=False, encoding="utf-8")
    print(f"⚠️ Se encontraron {len(duplicados)} registros duplicados por 'acuse_estatal'. Se guardaron en:")
    print(f"   {ruta_duplicados}")

    df_full = df_full.sort_values("__orden_original__").drop(columns=["__orden_original__"])
    df_full = df_full.drop_duplicates("acuse_estatal", keep="first")
else:
    print("❌ ERROR: No se encontró la columna 'acuse_estatal'. No se puede detectar duplicados.")

# 🧮 Paso 3: Corregir columnas con enteros declarados como decimales
columnas_a_convertir = [
    "dap_25_kg_anio_actual",
    "urea_25_kg_anio_actual",
    "dap_25_kg_remanente",
    "urea_25_kg_remanente",
    "superficie_apoyada"
]

for col in columnas_a_convertir:
    if col in df_full.columns:
        df_full[col] = pd.to_numeric(df_full[col], errors="coerce").fillna(0).astype("Int64")

# 🗃️ Obtener columnas de PostgreSQL
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'full_derechohabientes_2025'
        ORDER BY ordinal_position
    """))
    columnas_pg = [row[0] for row in result.fetchall()]

# 🧽 Reordenar columnas según la base
faltantes = [col for col in columnas_pg if col not in df_full.columns]
if faltantes:
    print(f"❌ Las siguientes columnas no están en el DataFrame: {faltantes}")
    raise Exception("🚫 No se puede continuar. Faltan columnas requeridas.")
df_full = df_full[columnas_pg]

# 🧾 Convertir fecha_entrega si existe
if "fecha_entrega" in df_full.columns:
    df_full["fecha_entrega"] = pd.to_datetime(df_full["fecha_entrega"], errors="coerce").dt.strftime("%Y-%m-%d")

# 💾 Exportar archivo temporal
ruta_temp = "/tmp/full_derechohabientes_2025_temp.csv"
df_full.to_csv(ruta_temp, index=False, header=False, encoding="utf-8", na_rep='')

# 🚀 Ejecutar COPY
try:
    with psycopg_conn, psycopg_conn.cursor() as cur:
        print("🗑️ Truncando tabla full_derechohabientes_2025...")
        cur.execute("TRUNCATE TABLE full_derechohabientes_2025;")
        print("📥 Importando datos con COPY...")
        with open(ruta_temp, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"COPY full_derechohabientes_2025 ({', '.join(columnas_pg)}) FROM STDIN WITH CSV",
                f
            )
    print(f"✅ Importación completada en la tabla 'full_derechohabientes_2025' ({len(df_full)} registros).")
except Exception as e:
    print(f"❌ Error durante la importación con COPY: {e}")

# ⏱️ Fin del proceso
fin = time.time()
print(f"⏱️ Tiempo total de ejecución: {round(fin - inicio, 2)} segundos.")
