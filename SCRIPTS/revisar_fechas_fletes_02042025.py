import pandas as pd
import glob
import os

# Ruta al archivo
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"
archivo_fletes = glob.glob(os.path.join(ruta_base, "*FERTILIZANTES-FLETES-NACIONAL-ANUAL*.csv"))[0]

# Cargar archivo sin transformar fechas
df = pd.read_csv(archivo_fletes, dtype=str, encoding="utf-8")

# Mostrar columnas que contienen la palabra 'fecha'
columnas_fecha = [col for col in df.columns if "fecha" in col.lower()]
print(f"\n📅 Columnas que contienen fechas:\n{columnas_fecha}\n")

# Mostrar los primeros valores únicos en cada columna de fecha
for col in columnas_fecha:
    print(f"🔎 Ejemplos en columna '{col}':")
    print(df[col].dropna().unique()[:10])  # Mostrar solo los primeros 10 valores no nulos
    print("-" * 50)

print("\n✅ Revisión de formatos de fecha terminada.")
