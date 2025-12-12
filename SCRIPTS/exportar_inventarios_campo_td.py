import pandas as pd
from conexion import engine  # ✅ Conexión centralizada

# Consulta SQL
query = """
select
  id_ceda_agricultura,
  fecha_ultimo_reporte,
   COALESCE(inventario_dap_ultimo, 3) AS dap_campo_ultimo,
  COALESCE(inventario_urea_ultimo, 3) AS urea_campo_ultimo
from
estadisticas_inventarios_campo
order by id_ceda_agricultura;
"""

# Leer los datos en un DataFrame
df = pd.read_sql(query, engine)

# Ruta del archivo CSV
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/inventarios_campo_td.csv"

# Exportar a CSV con codificación UTF-8
df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("✅ Archivo 'inventarios_campo_td.csv' exportado correctamente.")
