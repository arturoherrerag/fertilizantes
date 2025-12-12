import pdfplumber
import pandas as pd
import os

# Ruta al archivo PDF en tu Mac
archivo_pdf = "/Users/Arturo/Downloads/Distribución Fertilizantes 2025 UR-partidas.pdf"

# Ruta de salida del archivo Excel (en el mismo directorio)
archivo_salida = "/Users/Arturo/Downloads/Distribucion_Fertilizantes_2025_UR-partidas.xlsx"

# Lista para almacenar todas las filas de todas las páginas
todas_las_filas = []

# Abrir el PDF y extraer tablas
with pdfplumber.open(archivo_pdf) as pdf:
    for i, pagina in enumerate(pdf.pages):
        tablas = pagina.extract_tables()
        for tabla in tablas:
            for fila in tabla:
                if any(fila):  # Evitar filas completamente vacías
                    todas_las_filas.append(fila)

# Crear un DataFrame
df = pd.DataFrame(todas_las_filas)

# Intentar establecer encabezados si se detectan (ajusta el índice si es otra fila)
if len(df) > 1:
    df.columns = df.iloc[1]  # Encabezado en la segunda fila
    df = df.drop([0, 1]).reset_index(drop=True)

# Guardar como archivo Excel
df.to_excel(archivo_salida, index=False)

print(f"✅ Archivo Excel generado con éxito: {archivo_salida}")
