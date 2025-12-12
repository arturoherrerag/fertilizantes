import pandas as pd
from sqlalchemy import create_engine, text

# Configuración de la conexión a PostgreSQL
DB_USER = "postgres"
DB_PASSWORD = "Art4125r0"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "fertilizantes"

# Crear conexión a PostgreSQL
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Generar fechas para el año 2025
fechas = pd.date_range(start="2025-01-01", end="2025-12-31", freq='D')
df = pd.DataFrame({'fecha': fechas})
df['anio'] = df['fecha'].dt.year
df['mes'] = df['fecha'].dt.month
df['semana'] = df['fecha'].dt.isocalendar().week
df['dia'] = df['fecha'].dt.day

# Diccionarios de traducción manual
meses = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
    5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
    9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}
dias = {
    0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves',
    4: 'Viernes', 5: 'Sábado', 6: 'Domingo'
}

# Aplicar traducciones
df['nombre_mes'] = df['mes'].map(meses)
df['nombre_dia'] = df['fecha'].dt.weekday.map(dias)
df['es_fin_de_semana'] = df['nombre_dia'].isin(['Sábado', 'Domingo'])

# Crear tabla dim_fecha en PostgreSQL
create_table_sql = """
CREATE TABLE IF NOT EXISTS dim_fecha (
    fecha DATE PRIMARY KEY,
    anio INTEGER,
    mes INTEGER,
    nombre_mes TEXT,
    semana INTEGER,
    dia INTEGER,
    nombre_dia TEXT,
    es_fin_de_semana BOOLEAN
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table_sql))
    print("✅ Tabla dim_fecha creada correctamente.")
    
    # Borrar datos anteriores (si existen)
    conn.execute(text("DELETE FROM dim_fecha;"))
    print("🧹 Datos anteriores eliminados.")

# Insertar datos nuevos
df.to_sql('dim_fecha', engine, index=False, if_exists='append')
print("🚀 Tabla dim_fecha poblada con fechas del 2025.")
