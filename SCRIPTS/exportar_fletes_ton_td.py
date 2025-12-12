import pandas as pd
from conexion import engine  # ✅ Usando conexión centralizada

# Consulta a la vista fletes_ton_td
query = "SELECT * FROM fletes_ton_td;"

# Leer datos con pandas
df = pd.read_sql(query, engine)

# Ruta destino del archivo CSV
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/fletes_ton_td.csv"

# Guardar como CSV con codificación UTF-8
df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("✅ Archivo 'fletes_ton_td.csv' exportado correctamente.")
