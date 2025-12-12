import pandas as pd
from sqlalchemy import text
from conexion import engine, DB_NAME

# Ruta de salida
ruta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/abasto_y_remanente_x_dia_2025.csv"

# Exportar la vista a CSV
try:
    with engine.begin() as conn:
        df = pd.read_sql(text("SELECT * FROM abasto_y_remanente_x_dia_2025"), conn)
        df.to_csv(ruta_csv, index=False, encoding='utf-8-sig')
    print(f"✅ Vista 'abasto_y_remanente_x_dia_2025' exportada exitosamente a: {ruta_csv}")
except Exception as e:
    print(f"❌ Error durante la exportación: {e}")
