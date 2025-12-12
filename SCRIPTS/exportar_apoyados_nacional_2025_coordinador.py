import pandas as pd
from conexion import engine  # Asumiendo conexión SQLAlchemy centralizada

query = """
SELECT
f.id_persona as id_suri,
f.nombre,
f.primer_apellido as "primer apellido",
f.segundo_apellido as "segundo apellido",
f.curp_renapo as curp,
f.clave_estado_predio_capturada as cve_estado,
f.estado_predio_capturada as estado,
f.clave_municipio_predio_capturada as cve_municipio,
f.municipio_predio_capturada as municipio,
'N/A' as bancarizado,
'APOYADO' as estatus,
'Programa de Fertilizantes' as programa
FROM full_derechohabientes_2025 f
LEFT JOIN derechohabientes d
ON f.acuse_estatal=d.acuse_estatal
WHERE d.fecha_entrega >'2025-03-31' AND d.fecha_entrega <'2025-05-01';
"""

df = pd.read_sql(query, engine)

# Exportar respetando acentos y letra ñ
df.to_csv('/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/derechohabientes_apoyados_nacional_abril_2025.csv',
          index=False,
          encoding='utf-8-sig')  # ✅ UTF-8 con BOM para compatibilidad Excel
