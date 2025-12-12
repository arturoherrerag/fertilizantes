import pandas as pd
from sqlalchemy import create_engine

# Configuración de la conexión a PostgreSQL
DB_USER = "postgres"
DB_PASSWORD = "Art4125r0"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "fertilizantes"

# Crear la conexión
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Consulta a la vista fletes_conteo_td
query = "SELECT * FROM fletes_conteo_td;"

# Leer datos con pandas
df = pd.read_sql(query, engine)

# Ruta de salida para el archivo CSV
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/fletes_conteo_td.csv"

# Exportar a CSV con codificación UTF-8
df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("✅ Archivo 'fletes_conteo_td.csv' exportado correctamente.")
