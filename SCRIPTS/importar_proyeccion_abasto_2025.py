import pandas as pd
from sqlalchemy import text
from conexion import engine, DB_NAME

# Ruta al CSV
ruta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/Abasto_Programado_Fertilizante_2025.csv"

# Leer datos
df = pd.read_csv(ruta_csv)
df.columns = df.columns.str.lower().str.replace(" ", "_")

# ✅ Corregido: fechas con año de 2 dígitos
df['fecha'] = pd.to_datetime(df['fecha'], format="%d/%m/%y")

# Importar a PostgreSQL
try:
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE proyeccion_abasto_2025;"))
        df.to_sql("proyeccion_abasto_2025", con=conn, if_exists="append", index=False)
    print(f"✅ Datos importados correctamente a la tabla 'proyeccion_abasto_2025' en la base '{DB_NAME}'")
except Exception as e:
    print(f"❌ Error al importar los datos: {e}")
