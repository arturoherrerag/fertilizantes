import os
import pandas as pd
import glob
from datetime import datetime

# Rutas
carpeta_derechohabientes = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes/"
archivo_no_sincronizados = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/CORRECCIONES DERECHOHABIENTES/derechohabientes_no_sincronizados_nacional.xlsx"
carpeta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/CORRECCIONES DERECHOHABIENTES/"

# 1️⃣ Combinar archivos derechohabientes
archivos_csv = glob.glob(os.path.join(carpeta_derechohabientes, "*.CSV"))

df_derechohabientes = pd.concat(
    (
        pd.read_csv(archivo, encoding="utf-8", delimiter=",", dtype={12: str})
        .rename(columns=lambda col: col.lower().strip())
        for archivo in archivos_csv
    ),
    ignore_index=True
)

# Asegurar columna clave en minúsculas
if "acuse_estatal" not in df_derechohabientes.columns:
    raise ValueError("❌ No se encontró la columna 'acuse_estatal' en los derechohabientes.")

acuses_bd = set(df_derechohabientes["acuse_estatal"].dropna().astype(str).str.strip())

# 2️⃣ Leer archivo de no sincronizados
df_no_sync = pd.read_excel(archivo_no_sincronizados, dtype=str)
df_no_sync.columns = df_no_sync.columns.str.strip().str.lower()

# Buscar la columna correcta por nombre o posición
if "acuse_estatal" in df_no_sync.columns:
    acuses_archivo = df_no_sync["acuse_estatal"].astype(str).str.strip()
else:
    acuses_archivo = df_no_sync.iloc[:, 3].astype(str).str.strip()  # columna D es la 4ta (índice 3)

# 3️⃣ Identificar los acuses NO presentes
acuses_faltantes = acuses_archivo[~acuses_archivo.isin(acuses_bd)].drop_duplicates().reset_index(drop=True)

# 4️⃣ Exportar resultado
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
nombre_salida = f"acuses no sincronizados sigap_{timestamp}.csv"
ruta_salida = os.path.join(carpeta_salida, nombre_salida)

acuses_faltantes.to_csv(ruta_salida, index=False, header=["acuse_estatal"], encoding="utf-8")

print(f"✅ Acuses faltantes guardados en: {ruta_salida}")
print(f"📊 Total identificados: {len(acuses_faltantes)}")