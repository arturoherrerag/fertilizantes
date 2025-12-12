import pandas as pd
import os
import glob
import unicodedata
import re

# Recomendado: instalar dateutil si aún no lo tienes
# pip install python-dateutil
from dateutil import parser

# Ajusta la ruta a tu archivo CSV de incidentes
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"
file_list = glob.glob(os.path.join(ruta_base, "*-INCIDENTES-NACIONAL-ANUAL_*"))
if not file_list:
    print("❌ No se encontró ningún archivo con la máscara '*-INCIDENTES-NACIONAL-ANUAL_*'")
    exit()

file_path = file_list[0]

def normalizar_columna(col):
    col = col.strip().lower()
    col = ''.join(
        c for c in unicodedata.normalize('NFD', col)
        if unicodedata.category(c) != 'Mn'
    )
    col = re.sub(r'[^a-z0-9]+', '_', col)
    col = re.sub(r'[_]+', '_', col).strip('_')
    return col

print(f"ℹ️ Revisando archivo: {file_path}")

# 1) Leer el CSV con todo como texto
df = pd.read_csv(file_path, dtype=str, encoding='utf-8', delimiter=',')

# 2) Normalizar nombre de columnas
df.columns = [normalizar_columna(c) for c in df.columns]

col_fecha = "fecha_incidente"
if col_fecha not in df.columns:
    print(f"⚠️ La columna '{col_fecha}' no existe en el CSV.")
    exit()

# 3) Ver cuántos valores hay y muestra algunos
num_filas = len(df)
print(f"✅ Se leyeron {num_filas} filas total.")
print(f"🎯 Valores únicos en '{col_fecha}':\n{df[col_fecha].unique()}")
print("\n--- INTENTO DE PARSEO CON 'dateutil.parser.parse' ---")

errores = []
exitos = 0

# 4) Recorrer todas o un subset (si tu archivo es muy grande, usa .head(50) o similar)
for i, valor_str in enumerate(df[col_fecha]):  
    if i >= 50:
        # Muestra solo los primeros 50 para no saturar la salida
        break
    
    try:
        if pd.isna(valor_str):
            # Si la celda es NaN (pandas) o None, no se puede parsear
            print(f"Fila {i}: valor='{valor_str}' -> Es nulo/NaN, no se puede parsear.")
            errores.append(valor_str)
        else:
            dt = parser.parse(valor_str)  # dateutil intenta distintos formatos
            print(f"Fila {i}: valor='{valor_str}' -> Se parsea correctamente como: {dt}")
            exitos += 1
    except Exception as e:
        print(f"Fila {i}: valor='{valor_str}' -> ❌ Error: {e}")
        errores.append(valor_str)

print("\n--- Resumen del intento de parseo ---")
print(f" - Filas parseadas con éxito: {exitos}")
print(f" - Filas con error/nulo en '{col_fecha}': {len(errores)}")
if errores:
    print("❌ Ejemplos de valores problemáticos:")
    print(errores[:10])  # muestra los primeros 10 con error
