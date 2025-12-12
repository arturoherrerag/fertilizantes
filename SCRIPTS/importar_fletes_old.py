import os
import glob
import unicodedata
import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import Text, Integer, Numeric, TIMESTAMP, Date
from conexion import engine, DB_NAME

# -----------------------------------------------------------------------------
# Función para remover acentos y diacríticos de una cadena
# -----------------------------------------------------------------------------
def remover_acentos(cadena):
    if not isinstance(cadena, str):
        return cadena
    nfkd_form = unicodedata.normalize('NFD', cadena)
    return nfkd_form.encode('ASCII', 'ignore').decode('utf-8', 'ignore')


# -----------------------------------------------------------------------------
# 2. Rutas de archivos
# -----------------------------------------------------------------------------
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"
archivo_fletes = glob.glob(os.path.join(ruta_base, "*FLETES-NACIONAL-ANUAL*.csv"))[0]
archivo_eliminar = os.path.join(ruta_base, "eliminar_fletes.xlsx")
archivo_corregidos = os.path.join(ruta_base, "fletes_corregidos.csv")

# -----------------------------------------------------------------------------
# 3. Lectura y normalización del archivo principal de fletes
# -----------------------------------------------------------------------------
fletes_df = pd.read_csv(archivo_fletes, dtype=str, encoding="utf-8")

# Normalizar nombres de columnas (quitar acentos, minúsculas, y "_" en lugar de espacios)
nuevos_nombres = []
for col in fletes_df.columns:
    col_sin_acentos = remover_acentos(col).lower().strip()
    col_sin_acentos = col_sin_acentos.replace(" ", "_")
    nuevos_nombres.append(col_sin_acentos)
fletes_df.columns = nuevos_nombres

# -----------------------------------------------------------------------------
# 4. Eliminar registros según "eliminar_fletes.xlsx" (si existe)
# -----------------------------------------------------------------------------
if os.path.exists(archivo_eliminar):
    eliminar_df = pd.read_excel(archivo_eliminar, dtype=str)
    # Normalizar columnas en 'eliminar_df'
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
        print("⚠️ No se encontró la columna 'folio_del_flete' en alguno de los archivos para eliminar.")
else:
    print("⚠️ No se encontró el archivo 'eliminar_fletes.xlsx'. No se eliminaron registros.")

# -----------------------------------------------------------------------------
# 5. Añadir registros corregidos (si existe) con la misma limpieza
# -----------------------------------------------------------------------------
if os.path.exists(archivo_corregidos):
    corregidos_df = pd.read_csv(archivo_corregidos, dtype=str, encoding="utf-8")
    # Normalizar columnas en 'corregidos_df'
    nombres_corregidos = []
    for col in corregidos_df.columns:
        col_sin_acentos = remover_acentos(col).lower().strip()
        col_sin_acentos = col_sin_acentos.replace(" ", "_")
        nombres_corregidos.append(col_sin_acentos)
    corregidos_df.columns = nombres_corregidos
    
    antes_corregidos = len(fletes_df)
    # Concatenar ambos DF (principal + corregidos)
    fletes_df = pd.concat([fletes_df, corregidos_df], ignore_index=True)
    despues_corregidos = len(fletes_df)
    print(f"✅ Se añadieron {despues_corregidos - antes_corregidos} registros corregidos.")
else:
    print("⚠️ No se encontró el archivo 'fletes_corregidos.csv'. Se omitió su carga.")

# -----------------------------------------------------------------------------
# 6. Transformaciones de fechas y tipos (para el DF unificado)
#    Según tu verificación, el CSV usa formato 'YYYY-MM-DD HH:MM:SS'
#    - fecha_de_salida => date
#    - fecha_de_llegada => timestamp
#    - fecha_de_entrega => timestamp (si 'estatus' = 'autorizado' => NaT)
# -----------------------------------------------------------------------------
if "fecha_de_salida" in fletes_df.columns:
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
# 7. Conversión de columnas numéricas y texto
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
# 8. Crear/Asignar 'id_ceda_agricultura' según estatus
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
# 9. Verificación rápida
# -----------------------------------------------------------------------------
print("\nEjemplo de algunos registros post-limpieza:")
cols_mostrar = [
    "folio_del_flete","estatus","fecha_de_salida",
    "fecha_de_llegada","fecha_de_entrega","cdf_destino_original",
    "cdf_destino_final","id_ceda_agricultura"
]
existen = [c for c in cols_mostrar if c in fletes_df.columns]
print(fletes_df[existen].head(5))

print("\nConteo de valores nulos en columnas de fecha (si existen):")
for fecha_col in ["fecha_de_salida", "fecha_de_llegada", "fecha_de_entrega"]:
    if fecha_col in fletes_df.columns:
        print(f"  {fecha_col}: {fletes_df[fecha_col].isnull().sum()}")

# 9B. (Opcional) Guardar CSV temporal para revisión
archivo_limpio = os.path.join(ruta_base, "fletes_limpios_para_importar.csv")
fletes_df.to_csv(archivo_limpio, index=False, encoding="utf-8")
print(f"\n🚀 CSV temporal guardado en: {archivo_limpio}")

# -----------------------------------------------------------------------------
# 10. Definir correspondencia de columnas a tipos SQLAlchemy
# -----------------------------------------------------------------------------
dtype_sqlalchemy = {
    "folio_del_flete": Text,
    "estatus": Text,
    "producto": Text,
    "abreviacion_producto": Text,  # normalizado
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
    "bultos_por_anio": Integer,
    "id_ceda_agricultura": Text
}

# -----------------------------------------------------------------------------
# 11. Vaciar tabla 'fletes' y reinsertar (sustituir)
# -----------------------------------------------------------------------------
table_name = "fletes"

print(f"\n🗑 Vaciar la tabla '{table_name}'...")
with engine.connect() as conn:
    conn.execute(text(f"TRUNCATE TABLE {table_name};"))
    conn.commit()

print(f"⬆️ Insertando {len(fletes_df)} registros en la tabla '{table_name}'...")
fletes_df.to_sql(
    name=table_name,
    con=engine,
    if_exists="append",  # Se inserta en la tabla vacía
    index=False,
    dtype=dtype_sqlalchemy
)

print(f"\n✅ Se han sustituido los datos antiguos por los nuevos en la tabla '{table_name}' de la base '{DB_NAME}'.")
