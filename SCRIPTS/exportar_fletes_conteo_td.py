import pandas as pd
from conexion import engine  # ✅ Usamos el engine desde el archivo de conexión centralizado

# Consulta a la vista fletes_conteo_td
query = "SELECT * FROM fletes_conteo_td;"

# Leer datos con pandas
df = pd.read_sql(query, engine)

# Ruta de salida para el archivo CSV
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/fletes_conteo_td.csv"

# Exportar a CSV con codificación UTF-8
df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("✅ Archivo 'fletes_conteo_td.csv' exportado correctamente.")
