import pandas as pd

archivo = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_corregidos_2025.csv"

df = pd.read_csv(archivo, encoding="utf-8", dtype=str)
df.columns = df.columns.str.lower().str.strip()

if "fecha_entrega" in df.columns:
    print("🗓️ Primeros valores de 'fecha_entrega':")
    print(df["fecha_entrega"].dropna().head(10))
else:
    print("❌ La columna 'fecha_entrega' no se encuentra en el archivo.")
