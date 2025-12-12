import pandas as pd
from conexion import engine
import os

# Consulta
query = "SELECT * FROM proyeccion_abasto_x_dia_2025"

# Ruta de salida
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/proyeccion_abasto_x_dia_2025.csv"

# Exportación
try:
    df = pd.read_sql(query, con=engine)
    df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')
    print(f"✅ Exportación exitosa a: {ruta_salida}")
except Exception as e:
    print(f"❌ Error durante la exportación: {e}")
