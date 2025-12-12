import pandas as pd
from conexion import engine  # ✅ Importación del motor desde archivo centralizado

# Consulta a la vista red_td
query = "SELECT * FROM red_td;"

# Leer datos con pandas
df = pd.read_sql(query, engine)

# Ruta destino del archivo CSV
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/red_td.csv"

# Guardar como CSV con codificación UTF-8
df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("✅ Archivo 'red_td.csv' exportado correctamente.")
