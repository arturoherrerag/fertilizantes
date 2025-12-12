import os
from pathlib import Path

# Ruta base donde están los archivos
carpeta = Path("/Users/Arturo/AGRICULTURA/FERTILIZANTES/QUERIES")
archivo_salida = carpeta / "acumulado_queries.txt"

# Obtener todos los archivos .txt y .sql en la carpeta
archivos = sorted([f for f in carpeta.iterdir() if f.suffix in [".txt", ".sql"]])

with open(archivo_salida, "w", encoding="utf-8") as salida:
    for archivo in archivos:
        salida.write(f"-- CONTENIDO DE: {archivo.name}\n\n")
        with open(archivo, "r", encoding="utf-8") as f:
            salida.write(f.read())
        salida.write("\n\n" + "-"*80 + "\n\n")

print(f"Archivo generado: {archivo_salida}")
