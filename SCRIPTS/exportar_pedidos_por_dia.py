import pandas as pd
from sqlalchemy import text
from conexion import engine, DB_NAME

# Nombre de la vista y ruta del archivo CSV
vista = "pedidos_por_dia_2025"
ruta_salida = f"/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/{vista}.csv"

# Exportar a CSV
try:
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {vista}"), conn)
        df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')
    print(f"✅ Vista '{vista}' exportada correctamente a:\n{ruta_salida}")
except Exception as e:
    print(f"❌ Error al exportar la vista '{vista}': {e}")
