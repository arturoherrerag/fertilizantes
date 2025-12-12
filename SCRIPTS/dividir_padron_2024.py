import pandas as pd
import os

# Ruta del archivo original
ruta_csv = "/Users/Arturo/AGRICULTURA/2024/beneficiarios_2024.csv"

# Contar total de registros (sin encabezado)
with open(ruta_csv, 'r', encoding='utf-8') as f:
    total_lineas = sum(1 for _ in f)
    total_registros = total_lineas - 1

# Calcular tamaño por parte
registros_por_parte = total_registros // 4

# Crear carpeta de salida
carpeta_salida = "/Users/Arturo/AGRICULTURA/2024/beneficiarios_dividido"
os.makedirs(carpeta_salida, exist_ok=True)

# Variables de control
parte = 1
registros_acumulados = 0
registros_deseados = registros_por_parte
df_parte = []

# Leer por bloques
chunk_size = 100_000  # Ajusta según tu RAM
for chunk in pd.read_csv(ruta_csv, chunksize=chunk_size, encoding='utf-8'):
    df_parte.append(chunk)
    registros_acumulados += len(chunk)

    # Si es una de las tres primeras partes o ya es la última
    if registros_acumulados >= registros_deseados or parte == 4:
        df_total = pd.concat(df_parte, ignore_index=True)
        nombre_archivo = f"beneficiarios_2024_parte_{parte}.csv"
        ruta_salida = os.path.join(carpeta_salida, nombre_archivo)
        df_total.to_csv(ruta_salida, index=False, encoding='utf-8-sig')
        print(f"✅ Parte {parte} guardada con {len(df_total):,} registros.")
        
        parte += 1
        df_parte = []
        registros_acumulados = 0
        
        # Las siguientes solo necesitan exactamente una parte más
        if parte == 4:
            registros_deseados = total_registros  # lo que sobre
