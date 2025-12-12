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

# Ruta del archivo CSV
ruta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/red_distribucion_2024.csv"

# Leer el archivo CSV
df = pd.read_csv(ruta_csv, encoding='utf-8-sig')

# Asegurar que los campos tengan el tipo correcto antes de insertar
df['meta_derechohabientes'] = df['meta_derechohabientes'].astype(int)
df['meta_superficie_ha'] = df['meta_superficie_ha'].astype(int)
df['meta_dap_ton'] = df['meta_dap_ton'].astype(float)
df['meta_urea_ton'] = df['meta_urea_ton'].astype(float)

# Insertar sin modificar la estructura existente
df.to_sql('red_distribucion_2024', engine, if_exists='append', index=False)

print("✅ Datos importados correctamente en red_distribucion_2024 con estructura respetada.")
