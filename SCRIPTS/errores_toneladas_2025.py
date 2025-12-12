import os
import pandas as pd
import glob
import time
from sqlalchemy import text
from conexion import engine

# Iniciar medición de tiempo
inicio = time.time()

# 1️⃣ Leer y combinar todos los archivos CSV en derechohabientes
carpeta_derechohabientes = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes/"
archivos_csv = glob.glob(os.path.join(carpeta_derechohabientes, "*.csv"))

RENOMBRAR_COLUMNAS = {
    "urea_25_kg_anio_actual": "urea_anio_actual",
    "dap_25_kg_anio_actual": "dap_anio_actual",
    "dap_remanente_25_kg": "dap_remanente",
    "urea_remanente_25_kg": "urea_remanente"
}

df_list = []
for archivo in archivos_csv:
    df_temp = pd.read_csv(archivo, encoding="utf-8", delimiter=",", dtype={12: str})
    df_temp.columns = df_temp.columns.str.lower().str.strip()
    df_temp = df_temp.rename(columns=RENOMBRAR_COLUMNAS)
    df_temp['archivo_origen'] = os.path.basename(archivo)  # solo el nombre del archivo
    df_list.append(df_temp)

df_derechohabientes = pd.concat(df_list, ignore_index=True)

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

# 3️⃣ Agregar registros corregidos
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
columnas_integer = [
    "clave_estado_predio_capturada", "clave_municipio_predio_capturada", "clave_localidad_predio_capturada",
    "id_nu_solicitud", "id_cdf_entrega", "folio_persona", "clave_ddr", "clave_cader_ventanilla",
    "superficie_apoyada"
]
columnas_integer = [col for col in columnas_integer if col in df_derechohabientes.columns]
for col in columnas_integer:
    df_derechohabientes[col] = pd.to_numeric(df_derechohabientes[col], errors='coerce').astype("Int64")

columnas_int_con_decimales = ["dap_anio_actual", "urea_anio_actual", "dap_remanente", "urea_remanente"]
columnas_int_con_decimales = [col for col in columnas_int_con_decimales if col in df_derechohabientes.columns]
for col in columnas_int_con_decimales:
    df_derechohabientes[col] = pd.to_numeric(df_derechohabientes[col], errors='coerce').fillna(0).astype("Int64")

df_derechohabientes[["ton_dap_entregada", "ton_urea_entregada"]] = df_derechohabientes[
    ["ton_dap_entregada", "ton_urea_entregada"]
].apply(pd.to_numeric, errors='coerce').fillna(0)

if "fecha_entrega" in df_derechohabientes.columns:
    df_derechohabientes["fecha_entrega"] = pd.to_datetime(
        df_derechohabientes["fecha_entrega"],
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

# 5️⃣ Comparar contra padrones existentes en PostgreSQL
try:
    print("🔎 Comparando contra la tabla derechohabientes_padrones_2025...")

    query_padron = """
        SELECT acuse_estatal, dap_ton, urea_ton
        FROM derechohabientes_padrones_2025
    """
    df_padron = pd.read_sql(query_padron, engine)

    # Extraer columnas necesarias del DataFrame combinado
    df_comparacion = df_derechohabientes[[
        'acuse_estatal',
        'ton_dap_entregada',
        'ton_urea_entregada',
        'dap_anio_actual',
        'urea_anio_actual',
        'dap_remanente',
        'urea_remanente',
        'archivo_origen'
    ]].copy()

    # Combinar con padrón y detectar diferencias
    df_dif = df_comparacion.merge(df_padron, on='acuse_estatal', how='inner')

    df_dif = df_dif[
        (df_dif['ton_dap_entregada'] != df_dif['dap_ton']) |
        (df_dif['ton_urea_entregada'] != df_dif['urea_ton'])
    ]

    ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/dif_toneladas_entregadas_previas.csv"
    if df_dif.empty:
        print("✅ No se encontraron diferencias en toneladas respecto al padrón.")
    else:
        df_dif.to_csv(ruta_salida, index=False, encoding='utf-8-sig')
        print(f"⚠️ Diferencias encontradas: {len(df_dif)} registros exportados a:")
        print(f"📁 {ruta_salida}")

except Exception as e:
    print(f"❌ Error al comparar contra padrón: {e}")

# Tiempo total
fin = time.time()
print(f"⏱️ Tiempo total del proceso: {round(fin - inicio, 2)} segundos.")
