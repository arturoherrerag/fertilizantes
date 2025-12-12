import pandas as pd

# Ruta del archivo CSV a revisar
ruta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/1051-FERTILIZANTES-FLETES-NACIONAL-ANUAL_2025-03-09 18_52_01_.csv"

# Cargar el archivo CSV (leer todo como texto para evitar conversiones automáticas)
df = pd.read_csv(ruta_csv, dtype=str)

# Función para detectar tipo de dato predominante
def detectar_tipo_dato(serie):
    try:
        # Convertir a números enteros y verificar si no hay decimales
        if serie.dropna().str.isnumeric().all():
            return "INTEGER"
        # Convertir a float, si todos los valores pueden convertirse es un NUMERIC
        serie_float = pd.to_numeric(serie.dropna(), errors='coerce')
        if serie_float.notna().all():
            return "NUMERIC(10,2)"  # Asumimos 10 dígitos totales, 2 decimales
        # Detectar fechas
        serie_fecha = pd.to_datetime(serie.dropna(), errors='coerce')
        if serie_fecha.notna().all():
            return "DATE"
    except:
        pass
    return "TEXT"

# Calcular la longitud máxima de cada columna
longitudes = df.applymap(lambda x: len(str(x)) if pd.notna(x) else 0).max()

# Detectar el tipo de dato predominante en cada columna
tipos_dato = df.apply(detectar_tipo_dato)

# Crear un DataFrame con los resultados
resultado = pd.DataFrame({
    "Columna": df.columns,
    "Longitud Máxima": longitudes.values,
    "Tipo de Dato Sugerido": tipos_dato.values
})

# Mostrar los resultados
print(resultado)

# Guardar los resultados en un archivo CSV
resultado.to_csv("/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS/analisis_fletes.csv", index=False)
