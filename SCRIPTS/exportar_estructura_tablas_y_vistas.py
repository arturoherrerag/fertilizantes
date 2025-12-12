import os
import pandas as pd
from conexion import engine  # Asegúrate de que engine esté correctamente configurado

# Consulta para obtener estructura de tablas y vistas
query = """
SELECT 
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type,
    c.character_maximum_length,
    CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'YES' ELSE 'NO' END AS is_primary_key,
    CASE WHEN tc.constraint_type = 'FOREIGN KEY' THEN 'YES' ELSE 'NO' END AS is_foreign_key,
    kcu2.table_name AS referenced_table,
    kcu2.column_name AS referenced_column
FROM information_schema.columns c
LEFT JOIN information_schema.key_column_usage kcu
    ON c.table_name = kcu.table_name
    AND c.column_name = kcu.column_name
    AND c.table_schema = kcu.table_schema
LEFT JOIN information_schema.table_constraints tc
    ON kcu.constraint_name = tc.constraint_name
    AND kcu.table_schema = tc.table_schema
LEFT JOIN information_schema.referential_constraints rc
    ON tc.constraint_name = rc.constraint_name
LEFT JOIN information_schema.key_column_usage kcu2
    ON rc.unique_constraint_name = kcu2.constraint_name
    AND kcu.ordinal_position = kcu2.ordinal_position
WHERE c.table_schema = 'public'
ORDER BY c.table_name, c.ordinal_position;
"""

# Ruta de guardado
output_path = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/QUERIES/estructura_bd_fertilizantes.csv"

# Ejecutar y guardar
df = pd.read_sql(query, engine)
df.to_csv(output_path, index=False, encoding="utf-8")

print(f"✅ Estructura exportada correctamente a: {output_path}")
