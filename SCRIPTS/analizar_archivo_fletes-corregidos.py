import pandas as pd

# Ruta del archivo de corregidos
archivo_corregidos = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/fletes_corregidos.csv"

# Leer el archivo como texto
df = pd.read_csv(archivo_corregidos, dtype=str, encoding="utf-8")

# Revisar los primeros valores de las columnas de fecha
columnas_fecha = ["fecha_de_salida", "fecha_de_llegada", "fecha_de_entrega"]

for col in columnas_fecha:
    if col in df.columns:
        print(f"\n🕒 Primeros valores de la columna '{col}':")
        print(df[col].dropna().head(10))
