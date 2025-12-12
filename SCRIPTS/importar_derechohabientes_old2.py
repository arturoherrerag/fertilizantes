import os
import pandas as pd
import glob
import time
from sqlalchemy import text
from conexion import engine, psycopg_conn, DB_NAME

# Iniciar medición de tiempo
inicio = time.time()

# 1️⃣ Leer y combinar todos los archivos CSV en derechohabientes
carpeta_derechohabientes = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes/"
archivos_csv = glob.glob(os.path.join(carpeta_derechohabientes, "*.csv"))

df_derechohabientes = pd.concat(
    (pd.read_csv(archivo, encoding="utf-8", delimiter=",", dtype={12: str}) for archivo in archivos_csv),
    ignore_index=True
)

df_derechohabientes.columns = df_derechohabientes.columns.str.lower().str.strip()
print(f"📂 Se han combinado {len(archivos_csv)} archivos de la carpeta derechohabientes.")

# 2️⃣ Eliminar registros marcados con errores
archivo_eliminar = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_eliminar_2025.xlsx"
df_eliminar = pd.read_excel(archivo_eliminar, dtype=str)
df_eliminar.columns = df_eliminar.columns.str.lower().str.strip()

if "acuse_estatal" not in df_eliminar.columns:
    print("❌ ERROR: Falta columna 'acuse_estatal' en archivo de eliminación.")
    exit()

antes_eliminar = len(df_derechohabientes)
df_derechohabientes = df_derechohabientes[~df_derechohabientes["acuse_estatal"].isin(df_eliminar["acuse_estatal"])]
print(f"✅ Se eliminaron {antes_eliminar - len(df_derechohabientes)} registros.")

# 3️⃣ Agregar registros corregidos (ajustando solo la fecha_entrega de este lote)
archivo_corregidos = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_corregidos_2025.csv"
if os.path.exists(archivo_corregidos):
    df_corregidos = pd.read_csv(archivo_corregidos, encoding="utf-8", delimiter=",")
    df_corregidos.columns = df_corregidos.columns.str.lower().str.strip()

    if "fecha_entrega" in df_corregidos.columns:
        df_corregidos["fecha_entrega"] = pd.to_datetime(
            df_corregidos["fecha_entrega"],
            format="%d/%m/%y",
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    df_derechohabientes = pd.concat([df_derechohabientes, df_corregidos], ignore_index=True)
    print("✅ Se combinó el archivo derechohabientes_corregidos_2025.csv.")
else:
    print("⚠️ No se encontró archivo de registros corregidos, se omitió.")

# 4️⃣ Conversión de tipos
columnas_integer = ["clave_estado_predio_capturada", "clave_municipio_predio_capturada", "clave_localidad_predio_capturada",
                    "id_nu_solicitud", "id_cdf_entrega", "folio_persona", "clave_ddr", "clave_cader_ventanilla",
                    "superficie_apoyada"]
columnas_integer = [col for col in columnas_integer if col in df_derechohabientes.columns]
for col in columnas_integer:
    df_derechohabientes[col] = pd.to_numeric(df_derechohabientes[col], errors='coerce').astype("Int64")

# Columnas enteras que llegan como decimales
columnas_int_con_decimales = ["dap_anio_actual", "urea_anio_actual", "dap_remanente", "urea_remanente"]
columnas_int_con_decimales = [col for col in columnas_int_con_decimales if col in df_derechohabientes.columns]
for col in columnas_int_con_decimales:
    df_derechohabientes[col] = pd.to_numeric(df_derechohabientes[col], errors='coerce').fillna(0).astype("Int64")

# Columnas decimales reales
columnas_numeric = ["ton_dap_entregada", "ton_urea_entregada"]
df_derechohabientes[columnas_numeric] = df_derechohabientes[columnas_numeric].apply(pd.to_numeric, errors='coerce').fillna(0)

# Fechas generales (ya corregidas las del archivo corregido)
if "fecha_entrega" in df_derechohabientes.columns:
    df_derechohabientes["fecha_entrega"] = pd.to_datetime(
        df_derechohabientes["fecha_entrega"],
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

# 5️⃣ Reordenar columnas según estructura real de PostgreSQL
with engine.connect() as conn:
    result = conn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'derechohabientes';"
    ))
    columnas_postgres = [row[0] for row in result.fetchall()]
df_derechohabientes = df_derechohabientes[columnas_postgres]

# 6️⃣ Exportar CSV temporal para COPY
csv_temp_path = "/tmp/derechohabientes_temp.csv"
df_derechohabientes.to_csv(csv_temp_path, index=False, header=False, encoding="utf-8", na_rep='')

# 7️⃣ Cargar datos con COPY para eficiencia
try:
    with psycopg_conn, psycopg_conn.cursor() as cur:
        print("🗑️ Truncando tabla derechohabientes...")
        cur.execute("TRUNCATE TABLE derechohabientes;")
        print("📥 Importando datos con COPY...")
        with open(csv_temp_path, "r", encoding="utf-8") as f:
            cur.copy_expert(f"COPY derechohabientes ({', '.join(columnas_postgres)}) FROM STDIN WITH CSV", f)
    print(f"✅ Se han sustituido los datos antiguos por los nuevos en la tabla 'derechohabientes' de la base '{DB_NAME}'.")
    print(f"📊 Registros importados: {len(df_derechohabientes)}")
except Exception as e:
    print(f"❌ Error durante la importación con COPY: {e}")

# Tiempo total
fin = time.time()
print(f"⏱️ Tiempo total: {round(fin - inicio, 2)} segundos.")
