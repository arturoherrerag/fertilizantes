import os
import glob
import unicodedata
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.types import Text, Integer, Numeric, TIMESTAMP, Date

# Función para remover acentos
def remover_acentos(cadena):
    if not isinstance(cadena, str):
        return cadena
    nfkd_form = unicodedata.normalize('NFD', cadena)
    return nfkd_form.encode('ASCII', 'ignore').decode('utf-8', 'ignore')

# -----------------------------------------------------------------------------
# 1. Configuración de acceso a la base de datos PostgreSQL
# -----------------------------------------------------------------------------
DB_USER = "postgres"
DB_PASSWORD = "Art4125r0"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "fertilizantes"

connection_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_url)

# -----------------------------------------------------------------------------
# 2. Rutas de archivos
# -----------------------------------------------------------------------------
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"
archivo_fletes = glob.glob(os.path.join(ruta_base, "*FERTILIZANTES-FLETES-NACIONAL-ANUAL*.csv"))[0]
archivo_eliminar = os.path.join(ruta_base, "eliminar_fletes.xlsx")
archivo_corregidos = os.path.join(ruta_base, "fletes_corregidos.csv")

# -----------------------------------------------------------------------------
# 3. Lectura del archivo principal de fletes
# -----------------------------------------------------------------------------
fletes_df = pd.read_csv(archivo_fletes, dtype=str, encoding="utf-8")

# 3A. Normalizar nombres de columnas (quitar acentos, minúsculas, _ en vez de espacio)
nuevos_nombres = []
for col in fletes_df.columns:
    col_sin_acentos = remover_acentos(col).lower().strip()  # quita acentos, pasa a minúsculas
    col_sin_acentos = col_sin_acentos.replace(" ", "_")      # reemplaza espacios con _
    nuevos_nombres.append(col_sin_acentos)

fletes_df.columns = nuevos_nombres

# -----------------------------------------------------------------------------
# 4. Eliminar registros según "eliminar_fletes.xlsx"
# -----------------------------------------------------------------------------
if os.path.exists(archivo_eliminar):
    eliminar_df = pd.read_excel(archivo_eliminar, dtype=str)
    # Normalizar también nombres en eliminar_df
    cols_eliminar = []
    for col in eliminar_df.columns:
        col_norm = remover_acentos(col).lower().strip().replace(" ", "_")
        cols_eliminar.append(col_norm)
    eliminar_df.columns = cols_eliminar
    
    if "folio_del_flete" in eliminar_df.columns and "folio_del_flete" in fletes_df.columns:
        antes_eliminar = len(fletes_df)
        fletes_df = fletes_df[~fletes_df["folio_del_flete"].isin(eliminar_df["folio_del_flete"])]
        despues_eliminar = len(fletes_df)
        print(f"✅ Se eliminaron {antes_eliminar - despues_eliminar} registros basados en 'eliminar_fletes.xlsx'.")
    else:
        print("⚠️ No se encontró la columna 'folio_del_flete' en alguno de los DataFrames.")
else:
    print("⚠️ No se encontró el archivo 'eliminar_fletes.xlsx'. No se eliminaron registros.")

# -----------------------------------------------------------------------------
# 5. Añadir registros corregidos (si existen)
# -----------------------------------------------------------------------------
if os.path.exists(archivo_corregidos):
    corregidos_df = pd.read_csv(archivo_corregidos, dtype=str, encoding="utf-8")
    # Normalizar también los nombres de columnas
    cols_correg = []
    for col in corregidos_df.columns:
        col_norm = remover_acentos(col).lower().strip().replace(" ", "_")
        cols_correg.append(col_norm)
    corregidos_df.columns = cols_correg
    
    antes_corregidos = len(fletes_df)
    fletes_df = pd.concat([fletes_df, corregidos_df], ignore_index=True)
    despues_corregidos = len(fletes_df)
    
    print(f"✅ Se añadieron {despues_corregidos - antes_corregidos} registros corregidos.")
else:
    print("⚠️ No se encontró el archivo 'fletes_corregidos.csv'. No se agregaron registros corregidos.")

# -----------------------------------------------------------------------------
# 6. Manejo de fechas
#    NOTA: Los valores son 'YYYY-MM-DD HH:MM:SS' (verificado por tu script).
#    fecha_de_salida => date
#    fecha_de_llegada y fecha_de_entrega => timestamp
# -----------------------------------------------------------------------------
if "fecha_de_salida" in fletes_df.columns:
    # Parseamos con el formato correcto y nos quedamos con la parte de fecha
    fletes_df["fecha_de_salida"] = pd.to_datetime(
        fletes_df["fecha_de_salida"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    ).dt.date

if "fecha_de_llegada" in fletes_df.columns:
    fletes_df["fecha_de_llegada"] = pd.to_datetime(
        fletes_df["fecha_de_llegada"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    )

# Cuando estatus es 'autorizado', forzamos fecha_de_entrega a NaT (antes de parsear)
if "estatus" in fletes_df.columns and "fecha_de_entrega" in fletes_df.columns:
    mask_autorizado = fletes_df["estatus"].str.lower() == "autorizado"
    fletes_df.loc[mask_autorizado, "fecha_de_entrega"] = None

if "fecha_de_entrega" in fletes_df.columns:
    fletes_df["fecha_de_entrega"] = pd.to_datetime(
        fletes_df["fecha_de_entrega"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    )

# -----------------------------------------------------------------------------
# 7. Ajuste de tipos (numéricos, texto, etc.)
# -----------------------------------------------------------------------------
numeric_cols_3_decimals = ["toneladas_iniciales", "toneladas_en_el_destino", "toneladas_con_incidentes"]
for col in numeric_cols_3_decimals:
    if col in fletes_df.columns:
        fletes_df[col] = pd.to_numeric(fletes_df[col], errors="coerce").round(3)

if "ticket_bascula" in fletes_df.columns:
    fletes_df["ticket_bascula"] = fletes_df["ticket_bascula"].astype(str)

if "telefono_operador" in fletes_df.columns:
    fletes_df["telefono_operador"] = fletes_df["telefono_operador"].astype(str)

# -----------------------------------------------------------------------------
# 8. Creación de 'id_ceda_agricultura'
# -----------------------------------------------------------------------------
if "id_ceda_agricultura" not in fletes_df.columns:
    fletes_df["id_ceda_agricultura"] = None

if "estatus" in fletes_df.columns:
    # ENTREGADO => cdf_destino_final
    if "cdf_destino_final" in fletes_df.columns:
        mask_entregado = fletes_df["estatus"].str.lower() == "entregado"
        fletes_df.loc[mask_entregado, "id_ceda_agricultura"] = fletes_df["cdf_destino_final"]
    # AUTORIZADO => cdf_destino_original
    if "cdf_destino_original" in fletes_df.columns:
        mask_autorizado = fletes_df["estatus"].str.lower() == "autorizado"
        fletes_df.loc[mask_autorizado, "id_ceda_agricultura"] = fletes_df["cdf_destino_original"]

# -----------------------------------------------------------------------------
# 9. Vista previa
# -----------------------------------------------------------------------------
print("\nEjemplo de algunos registros post-limpieza:")
columnas_mostrar = [
    "folio_del_flete", "estatus", "fecha_de_salida",
    "fecha_de_llegada", "fecha_de_entrega", "cdf_destino_original",
    "cdf_destino_final", "id_ceda_agricultura"
]
cols_existen = [c for c in columnas_mostrar if c in fletes_df.columns]
print(fletes_df[cols_existen].head(5))

if any(c in fletes_df.columns for c in ["fecha_de_salida","fecha_de_llegada","fecha_de_entrega"]):
    print("\nConteo de valores nulos en las columnas de fecha:")
    for c in ["fecha_de_salida","fecha_de_llegada","fecha_de_entrega"]:
        if c in fletes_df.columns:
            print(f"  {c}: {fletes_df[c].isnull().sum()}")

# (Opcional) CSV temporal
archivo_limpio = os.path.join(ruta_base, "fletes_limpios_para_importar.csv")
fletes_df.to_csv(archivo_limpio, index=False, encoding="utf-8")
print(f"\n🚀 CSV temporal guardado en: {archivo_limpio}")

# -----------------------------------------------------------------------------
# 10. Diccionario de tipos con los nombres YA normalizados
# -----------------------------------------------------------------------------
dtype_sqlalchemy = {
    "folio_del_flete": Text,
    "estatus": Text,
    "producto": Text,
    "abreviacion_producto": Text,  # <<---- SIN acento
    "toneladas_iniciales": Numeric(10, 3),
    "bultos_iniciales": Integer,
    "bultos_en_destino": Integer,
    "ticket_bascula": Text,
    "estado_procedencia": Text,
    "toneladas_en_el_destino": Numeric(10, 3),
    "fecha_de_salida": Date,
    "cdf_destino_original": Text,
    "cdf_destino_final": Text,
    "fecha_de_llegada": TIMESTAMP,
    "fecha_de_entrega": TIMESTAMP,
    "toneladas_con_incidentes": Numeric(10, 3),
    "estatus_de_recepcion_incidente": Text,
    "descripcion": Text,
    "destino_final": Text,
    "nombre_operador": Text,
    "telefono_operador": Text,
    "placas_transporte": Text,
    "tipo_transporte": Text,
    "estado_llegada": Text,
    "bultos_por_anio": Integer,   # igualmente normalizado
    "id_ceda_agricultura": Text
}

# -----------------------------------------------------------------------------
# 11. Insertar en PostgreSQL
# -----------------------------------------------------------------------------
table_name = "fletes"
fletes_df.to_sql(
    name=table_name,
    con=engine,
    if_exists="append",  # o "replace" si quieres crear de cero
    index=False,
    dtype=dtype_sqlalchemy
)

print(f"\n✅ Datos insertados/actualizados en la tabla '{table_name}' de la base '{DB_NAME}'.")
