import pandas as pd

# Ruta del archivo CSV de derechohabientes
ruta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes/1051-FERTILIZANTES-BENEFICIARIOS-NACIONAL-PRIMER CORTE_2025-03-09 14_45_47_.csv"

# Cargar el archivo solo con los encabezados
df = pd.read_csv(ruta_csv, nrows=0)

# Mostrar los nombres de las columnas
print("📌 Encabezados en el archivo CSV:")
print(df.columns.tolist())
