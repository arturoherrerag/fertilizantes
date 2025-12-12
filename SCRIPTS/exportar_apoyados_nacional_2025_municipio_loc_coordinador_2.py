import pandas as pd
from conexion import engine  # Conexión SQLAlchemy centralizada

# Consulta SQL
query = """
SELECT
d.clave_estado_predio_capturada as "Clave de estado",
d.estado_predio_capturada as "Estado",
d.clave_municipio_predio_capturada as "Clave de municipio",
d.municipio_predio_capturada as "Municipio",
d.curp_renapo as "CURP",
CASE
    WHEN d.sn_segundo_apellido IS NULL OR TRIM(d.sn_segundo_apellido) = '' THEN
      d.ln_nombre || ' ' || d.sn_primer_apellido
    ELSE
      d.ln_nombre || ' ' || d.sn_primer_apellido || ' ' || d.sn_segundo_apellido
  END AS "Nombre de completo"
FROM
derechohabientes d
LEFT JOIN
full_derechohabientes_2025 f
ON d.acuse_estatal=f.acuse_estatal;
"""

# Leer datos
print("📥 Ejecutando consulta SQL...")
df = pd.read_sql(query, engine)
total_registros = len(df)
mitad = total_registros // 2

# Dividir en dos partes iguales
df1 = df.iloc[:mitad]
df2 = df.iloc[mitad:]

# Rutas de salida
ruta_1 = '/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/derechohabientes_apoyados_nacional__03062025_parte1.csv'
ruta_2 = '/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/derechohabientes_apoyados_nacional__03062025_parte2.csv'

# Exportar
print(f"📤 Exportando {len(df1)} registros a: {ruta_1}")
df1.to_csv(ruta_1, index=False, encoding='utf-8-sig')

print(f"📤 Exportando {len(df2)} registros a: {ruta_2}")
df2.to_csv(ruta_2, index=False, encoding='utf-8-sig')

print("✅ Exportación completada en 2 partes iguales.")
