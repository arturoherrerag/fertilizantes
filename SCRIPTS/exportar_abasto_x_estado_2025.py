import pandas as pd
from conexion import engine  # Asegúrate de que 'conexion.py' tenga el objeto engine definido

# Nombre de la vista
vista_sql = "entradas_por_estado_td"

# Ruta de salida
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/entradas_por_estado_td.csv"

try:
    print(f"📤 Exportando vista '{vista_sql}' desde PostgreSQL...")

    # Leer los datos desde la vista
    df = pd.read_sql(f"SELECT * FROM {vista_sql};", engine)

    # Exportar a CSV con codificación UTF-8 con BOM (compatible con Excel)
    df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

    print(f"✅ Vista '{vista_sql}' exportada correctamente a:\n{ruta_salida}")

except Exception as e:
    print(f"❌ Error al exportar la vista '{vista_sql}': {e}")
