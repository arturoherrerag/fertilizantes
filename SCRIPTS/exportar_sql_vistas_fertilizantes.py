import os

# Ruta de tu carpeta QUERIES
carpeta_queries = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/QUERIES"
archivo_salida = os.path.join(carpeta_queries, "vistas_fertilizantes.sql")

# Buscar todos los archivos .txt que empiezan con "vista"
archivos_vistas = sorted([
    f for f in os.listdir(carpeta_queries)
    if f.lower().startswith("vista") and f.endswith(".txt")
])

# Unir contenido de todos los archivos en uno solo
with open(archivo_salida, 'w', encoding='utf-8') as salida:
    for archivo in archivos_vistas:
        ruta = os.path.join(carpeta_queries, archivo)
        with open(ruta, 'r', encoding='utf-8') as f:
            salida.write(f"-- {archivo} --\n")
            salida.write(f.read())
            salida.write(f"\n-- Fin de {archivo} --\n\n")

print(f"✅ Archivo generado: {archivo_salida}")
