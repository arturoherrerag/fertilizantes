import pandas as pd
import csv
from sqlalchemy import text
import os

# ✅ Importar la conexión centralizada
from conexion import engine

# Ruta del archivo CSV
file_path = os.path.expanduser("~/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/red_distribucion_2025.csv")

# Verifica que exista
if not os.path.exists(file_path):
    print(f"❌ Archivo no encontrado: {file_path}")
    exit()

# Cargar CSV usando pandas y soporte para campos de texto largos
df = pd.read_csv(
    file_path,
    encoding="utf-8",
    delimiter=",",
    quoting=csv.QUOTE_MINIMAL,
    quotechar='"',
    skip_blank_lines=True
)

# Reemplazo de valores no válidos
df.replace(["N/A", "NA", "n/a", "-"], None, inplace=True)

# Columnas numéricas que deben limpiarse
columnas_numericas = [
    "meta_derechohabientes", "meta_superficie_ha", "meta_dap_ton", "meta_urea_ton", "meta_total_ton",
    "largo_m", "ancho_m", "alto_m", "altura_estiba_m",
    "tonelaje_alm_dap", "tonelaje_alm_urea", "tonelaje_alm_total",
    "latitud", "longitud", "traspaleo_latitud", "traspaleo_longitud",
    "capacidad_descarga", "prioridad"
]

for columna in columnas_numericas:
    if columna in df.columns:
        df[columna] = df[columna].astype(str).str.replace(",", "", regex=True).str.strip()
        df[columna] = pd.to_numeric(df[columna], errors="coerce")

# Verificación inicial
print("✅ Columnas cargadas:", df.columns.tolist())
print(df[columnas_numericas].head(5))

try:
    with engine.begin() as conn:
        # Insertar datos directamente
        df.to_sql("red_distribucion", conn, if_exists="append", index=False)
        print("✅ Datos insertados correctamente en 'red_distribucion'.")

except Exception as e:
    print(f"❌ Error durante la carga: {e}")
