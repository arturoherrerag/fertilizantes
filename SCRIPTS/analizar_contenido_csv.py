import pandas as pd

# Ruta al archivo
ruta_csv = "/Users/Arturo/AGRICULTURA/2024/beneficiarios_2024.csv"

# Mostrar columnas y contar registros sin cargar todo en memoria
def analizar_csv(ruta):
    # Leer solo la primera fila para obtener columnas
    columnas = pd.read_csv(ruta, nrows=0).columns.tolist()
    
    # Contar número de registros (sin encabezado)
    with open(ruta, 'r', encoding='utf-8') as f:
        total_lineas = sum(1 for _ in f)
        total_registros = total_lineas - 1  # Restamos encabezado

    print("🔹 Nombres de las columnas:")
    for col in columnas:
        print(f"- {col}")

    print(f"\n🔹 Total de registros (sin contar encabezado): {total_registros:,}")

# Ejecutar análisis
analizar_csv(ruta_csv)
