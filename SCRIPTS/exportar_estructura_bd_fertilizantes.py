import pandas as pd
from conexion import engine  # Usamos tu conexión centralizada

# Consulta para obtener la estructura de columnas, tipos, llaves primarias y foráneas
query = """
SELECT 
    c.table_name,
    CASE 
        WHEN t.relkind = 'r' THEN 'tabla'
        WHEN t.relkind = 'v' THEN 'vista'
        WHEN t.relkind = 'm' THEN 'vista materializada'
        ELSE t.relkind
    END AS tipo_estructura,
    c.column_name,
    c.data_type,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale,
    c.is_nullable,
    CASE WHEN pk.column_name IS NOT NULL THEN 'Sí' ELSE '' END AS es_llave_primaria,
    fk.constraint_name AS llave_foranea,
    fk.foreign_table_name AS tabla_referida,
    fk.foreign_column_name AS columna_referida
FROM information_schema.columns c
JOIN pg_class t ON t.relname = c.table_name
LEFT JOIN (
    SELECT 
        tc.table_name, 
        kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name
LEFT JOIN (
    SELECT
        kcu.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        rc.constraint_name
    FROM information_schema.referential_constraints rc
    JOIN information_schema.key_column_usage kcu ON rc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage ccu ON rc.unique_constraint_name = ccu.constraint_name
) fk ON c.table_name = fk.table_name AND c.column_name = fk.column_name
WHERE c.table_schema = 'public'
ORDER BY tipo_estructura, c.table_name, c.ordinal_position;
"""

# Ejecutar la consulta y guardar en DataFrame
df = pd.read_sql(query, engine)

# Ruta de salida
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/estructura_actual_bd.csv"
df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("✅ Estructura exportada en:", ruta_salida)
