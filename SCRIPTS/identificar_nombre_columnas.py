import pandas as pd
import os

# Ajusta la ruta a tu CSV real
ruta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/1051-FERTILIZANTES-TRANSFERENCIAS-NACIONAL-ANUAL_2025-03-08 14_32_58_2025_TR.csv"

def ver_nombres_columnas(ruta):
    # Lee el CSV con la codificación que uses normalmente
    df = pd.read_csv(ruta, dtype=str, encoding="utf-8")

    print("\nNombres de columnas leídas (tal cual las ve pandas):")
    for col in df.columns:
        print(repr(col))

if __name__ == "__main__":
    if os.path.exists(ruta_csv):
        ver_nombres_columnas(ruta_csv)
    else:
        print(f"El archivo '{ruta_csv}' no existe.")
