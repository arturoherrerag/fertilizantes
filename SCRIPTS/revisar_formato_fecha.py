import pandas as pd

archivo_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes/1051-FERTILIZANTES-BENEFICIARIOS-NACIONAL-PRIMER CORTE_2025-03-09 14_45_47_.csv"

df = pd.read_csv(archivo_csv, encoding="utf-8", delimiter=",")

# Muestra las primeras filas y revisa los tipos de datos
print(df.head())
print(df.dtypes)
