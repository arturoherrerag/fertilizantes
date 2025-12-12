# archivo: derechohabientes_tabasco_2025_sexo.py

import pandas as pd
import os
from conexion import engine

# Ruta de salida
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS"
nombre_archivo = "derechohabientes_apoyados_tabasco.csv"
ruta_completa = os.path.join(ruta_salida, nombre_archivo)

# Consulta: obtener todos los campos de la tabla derechohabientes para Tabasco
query = """
SELECT dh.*
FROM derechohabientes dh
JOIN red_distribucion rd
  ON dh.cdf_entrega = rd.id_ceda_agricultura
WHERE UPPER(rd.estado) = 'TABASCO';
"""

# Leer el resultado
df = pd.read_sql(query, engine)

# Crear columna 'sexo' a partir de CURP (posición 11 = índice 10)
df["sexo"] = df["curp_solicitud"].str.upper().str[10]

# Exportar archivo
df.to_csv(ruta_completa, index=False, encoding="utf-8")
print(f"✅ Archivo generado exitosamente: {ruta_completa}")
