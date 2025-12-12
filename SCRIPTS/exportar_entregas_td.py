import pandas as pd
from conexion import engine  # ✅ Usamos engine desde archivo centralizado

# Consulta a la vista entregas_td
query = "SELECT * FROM entregas_td;"

# Leer los datos en un DataFrame
df = pd.read_sql(query, engine)

# Ruta de salida para el archivo CSV
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/entregas_td.csv"

# Exportar a CSV con codificación UTF-8
df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("✅ Archivo 'entregas_td.csv' exportado correctamente.")
