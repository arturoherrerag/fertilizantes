import pandas as pd
from conexion import engine  # Usa tu engine centralizado con SQLAlchemy

# Nombre de la vista
vista_sql = "avances_2025"

# Ruta de salida
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/avances_2025.csv"

try:
    print(f"📤 Exportando vista '{vista_sql}' desde PostgreSQL...")
    
    # Leer los datos desde la vista
    df = pd.read_sql(f"SELECT * FROM {vista_sql};", engine)

    # Exportar a CSV con codificación UTF-8 con BOM para compatibilidad con Excel
    df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

    print(f"✅ Vista '{vista_sql}' exportada correctamente a:\n{ruta_salida}")

except Exception as e:
    print(f"❌ Error al exportar la vista '{vista_sql}': {e}")
