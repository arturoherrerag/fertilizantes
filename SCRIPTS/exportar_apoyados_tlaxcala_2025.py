import pandas as pd
from conexion import engine  # Asumiendo conexión SQLAlchemy centralizada

query = """
SELECT
  id_persona as id_sur,
  nombre,
  primer_apellido as "primer apellido",
  segundo_apellido as "segundo apellido",
  curp_renapo as curp,
  clave_estado_predio_capturada as cve_estado,
  estado_predio_capturada as estado,
  clave_municipio_predio_capturada as cve_municipio,
  municipio_predio_capturada as municipio,
  'N/A' as bancarizado,
  'APOYADO' as estatus,
  'Programa de Fertilizantes' as programa
FROM full_derechohabientes_2025
WHERE estado_predio_capturada = 'TLAXCALA'
  AND fecha_entrega > '2025-03-31'
  AND fecha_entrega < '2025-05-01';
"""

df = pd.read_sql(query, engine)

# Exportar respetando acentos y letra ñ
df.to_csv('/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/derechohabientes_tlaxcala.csv',
          index=False,
          encoding='utf-8-sig')  # ✅ UTF-8 con BOM para compatibilidad Excel
