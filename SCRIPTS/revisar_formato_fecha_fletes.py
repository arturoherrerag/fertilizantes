import os
import unicodedata
import pandas as pd

# Ajusta la ruta a tu archivo, por ejemplo:
ruta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/1051-FERTILIZANTES-FLETES-NACIONAL-ANUAL_2025-03-09 18_52_01_.csv"

def remover_acentos(cadena: str) -> str:
    if not isinstance(cadena, str):
        return cadena
    # Normaliza la cadena separando los acentos
    nfkd_form = unicodedata.normalize('NFD', cadena)
    # Elimina los caracteres de combinación (acentos)
    sin_acentos = nfkd_form.encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return sin_acentos

# 1) Leer el CSV
df = pd.read_csv(ruta_csv, dtype=str, encoding='utf-8')

print("\n--- Nombres de columnas ORIGINALES ---")
for col in df.columns:
    print(repr(col))

# 2) Normalizar nombres (remover acentos, minúsculas, _ en vez de espacio)
nuevos_nombres = []
for col in df.columns:
    col_sin_acentos = remover_acentos(col).lower().strip()
    col_sin_acentos = col_sin_acentos.replace(" ", "_")
    nuevos_nombres.append(col_sin_acentos)

df.columns = nuevos_nombres

print("\n--- Nombres de columnas DESPUES de remover acentos y espacios ---")
for col in df.columns:
    print(repr(col))

# 3) Imprimir primeros 10 valores de las columnas de fecha
for fecha_col in ["fecha_de_salida", "fecha_de_llegada", "fecha_de_entrega"]:
    if fecha_col in df.columns:
        print(f"\n--- Primeros 10 valores de la columna '{fecha_col}' ---")
        print(df[fecha_col].head(10))
