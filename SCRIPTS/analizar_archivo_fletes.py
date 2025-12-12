import pandas as pd
import glob
import os

# ✅ Definir correctamente la ruta
ruta_archivo = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP"

# ✅ Buscar archivo CSV específico basado en el patrón de nombre
archivos_csv = glob.glob(os.path.join(ruta_archivo, "*FERTILIZANTES-FLETES-NACIONAL-ANUAL*.csv"))

if len(archivos_csv) == 0:
    raise FileNotFoundError("No se encontró ningún archivo con ese patrón.")
else:
    archivo_csv = archivos_csv[0]

print(f"📂 Archivo analizado: {archivo_csv}")

# ✅ Leer archivo CSV
df = pd.read_csv(archivo_csv, dtype=str)

# ✅ Función robusta para detectar tipos de dato predominantes
def detectar_tipo_dato(serie):
    if serie.dropna().str.match(r'^\d+$').all():
        return 'INTEGER'
    elif serie.dropna().str.match(r'^\d+\.\d+$').all():
        return 'NUMERIC(10,2)'
    elif pd.to_datetime(serie.dropna(), format='%Y-%m-%d %H:%M:%S', errors='coerce').notna().all():
        return 'TIMESTAMP'
    elif pd.to_datetime(serie.dropna(), format='%Y-%m-%d', errors='coerce').notna().all():
        return 'DATE'
    else:
        return 'TEXT'

# ✅ Aplicar función para cada columna
tipos_datos = df.apply(detectar_tipo_dato)

# ✅ Longitudes máximas de texto por columna
longitudes_maximas = df.apply(lambda col: col.dropna().map(lambda x: len(str(x))).max())

# ✅ Conteo claro de valores nulos por columna
valores_nulos = df.isnull().sum()

# ✅ Conteo claro de valores únicos por columna
valores_unicos = df.nunique()

# ✅ Consolidar resultados claramente en DataFrame final
resultado = pd.DataFrame({
    "Columna": df.columns,
    "Tipo de Dato Sugerido": tipos_datos,
    "Longitud Máxima": longitudes_maximas,
    "Valores Nulos": valores_nulos,
    "Valores Únicos": valores_unicos
})

print(resultado)

# ✅ Guardar resultados claramente en CSV
ruta_resultado = os.path.join(ruta_archivo, "analisis_detallado_fletes.csv")
resultado.to_csv(ruta_resultado, index=False)

print(f"✅ Resultados guardados correctamente en: {ruta_resultado}")
