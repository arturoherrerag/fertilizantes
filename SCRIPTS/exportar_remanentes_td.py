import pandas as pd
from conexion import engine  # ✅ Usamos el motor centralizado

# Consulta a la vista remanentes_td
query = "SELECT * FROM remanentes_td;"

# Leer datos con pandas
df = pd.read_sql(query, engine)

# Ruta destino del archivo CSV
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/remanentes_td.csv"

# Guardar como CSV con codificación UTF-8
df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("✅ Archivo 'remanentes_td.csv' exportado correctamente.")
