import os
import glob
import pandas as pd

# Ajusta según necesites
archivo_original_pattern = "*_rem.csv"            # Patron para el CSV original
archivo_corregido = "remanentes_corregido.csv"    # Nombre exacto o patrón para el CSV corregido

# Columna a revisar
col_fecha = "fecha_de_salida"

def revisar_formato_fecha(df, col, date_format):
    """
    Intenta parsear la columna 'col' de df con 'date_format'.
    Devuelve un DF con las filas que NO se pudieron parsear.
    """
    # Convertimos a datetime con errors='coerce'
    parsed = pd.to_datetime(df[col], format=date_format, errors='coerce')
    
    # Mask de filas que quedaron en NaT => no se pudo parsear
    mask_falla = parsed.isna()
    df_fallas = df[mask_falla].copy()
    return df_fallas

def main():
    # 1) Buscar el archivo original con el patron archivo_original_pattern
    ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"
    archivos_originales = glob.glob(os.path.join(ruta_base, archivo_original_pattern))
    
    if not archivos_originales:
        print(f"No se encontró ningún archivo con el patrón {archivo_original_pattern}.")
        return
    
    archivo_original = archivos_originales[0]
    print(f"✅ Archivo ORIGINAL encontrado: {archivo_original}")
    
    # 2) Leer el original
    df_original = pd.read_csv(archivo_original, dtype=str, encoding="utf-8")
    
    # 2A) Imprimir primeras filas de la columna fecha
    print("\n=== Valores iniciales de la columna en archivo ORIGINAL ===")
    if col_fecha in df_original.columns:
        print(df_original[col_fecha].head(20))
    else:
        print(f"La columna '{col_fecha}' no existe en el CSV original.")
    
    # 2B) Revisar formato DD/MM/YYYY
    print(f"\nRevisando formato '%d/%m/%Y' en archivo ORIGINAL para la columna '{col_fecha}'...")
    if col_fecha in df_original.columns:
        df_fallas_ori = revisar_formato_fecha(df_original, col_fecha, "%d/%m/%Y")
        
        print(f"Filas que NO pudieron parsearse en '{archivo_original}' con formato '%d/%m/%Y': {len(df_fallas_ori)}")
        if not df_fallas_ori.empty:
            # Mostramos algunas filas problemáticas
            print(df_fallas_ori[[col_fecha]].head(20))
    else:
        print("No se revisa el formato pues la columna no existe.")
    
    # 3) Leer el archivo corregido
    ruta_corregido = os.path.join(ruta_base, archivo_corregido)
    if not os.path.exists(ruta_corregido):
        print(f"\n⚠️ No se encontró el archivo corregido: {ruta_corregido}")
        return
    
    print(f"\n✅ Archivo CORREGIDO encontrado: {ruta_corregido}")
    df_corregido = pd.read_csv(ruta_corregido, dtype=str, encoding="utf-8")
    
    # 3A) Imprimir primeras filas de la columna fecha
    print("\n=== Valores iniciales de la columna en archivo CORREGIDO ===")
    if col_fecha in df_corregido.columns:
        print(df_corregido[col_fecha].head(20))
    else:
        print(f"La columna '{col_fecha}' no existe en el CSV corregido.")
    
    # 3B) Revisar formato DD/MM/YYYY en archivo corregido
    print(f"\nRevisando formato '%d/%m/%Y' en archivo CORREGIDO para la columna '{col_fecha}'...")
    if col_fecha in df_corregido.columns:
        df_fallas_cor = revisar_formato_fecha(df_corregido, col_fecha, "%d/%m/%Y")
        
        print(f"Filas que NO pudieron parsearse en '{archivo_corregido}' con formato '%d/%m/%Y': {len(df_fallas_cor)}")
        if not df_fallas_cor.empty:
            # Mostramos algunas filas problemáticas
            print(df_fallas_cor[[col_fecha]].head(20))
    else:
        print("No se revisa el formato pues la columna no existe en el archivo corregido.")
    
    print("\n✅ Revisión terminada. Si hay filas con problemas, revisa su formato o datos vacíos.")

if __name__ == "__main__":
    main()
