import os
import glob
import unicodedata
import pandas as pd
import re
from sqlalchemy import text
from sqlalchemy.types import Text, Integer, Numeric, TIMESTAMP, Date
from conexion import engine, DB_NAME

# =============================================================================
# FUNCION PARA REMOVER ACENTOS
# =============================================================================
def remover_acentos(cadena):
    if not isinstance(cadena, str):
        return cadena
    nfkd_form = unicodedata.normalize('NFD', cadena)
    return nfkd_form.encode('ASCII', 'ignore').decode('utf-8', 'ignore')

# =============================================================================
# 1. RUTAS DE ARCHIVOS
# =============================================================================
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"
archivo_remanentes = glob.glob(os.path.join(ruta_base, "*TRANSFERENCIAS-NACIONAL-ANUAL_2025_rem.csv"))[0]
archivo_eliminar = os.path.join(ruta_base, "eliminar_remanentes.xlsx")
archivo_corregidos = os.path.join(ruta_base, "remanentes_corregido.csv")

# =============================================================================
# FASE A: LECTURA DEL CSV E IMPRESION DE COLUMNAS INICIALES (ARCHIVO ORIGINAL)
# =============================================================================
remanentes_df = pd.read_csv(archivo_remanentes, dtype=str, encoding="utf-8")
print("FASE A) Columnas inmediatamente tras la lectura (ORIGINAL):\n", list(remanentes_df.columns), "\n")

# =============================================================================
# FASE B: RENOMBRADO PUNTUAL PREVIO (SI EXISTE "estatus_de_recepciÃ³n")
# =============================================================================
if "estatus_de_recepciÃ³n" in remanentes_df.columns:
    remanentes_df.rename(columns={"estatus_de_recepciÃ³n": "estatus_de_recepción"}, inplace=True)

print("FASE B) Columnas tras renombrar 'estatus_de_recepciÃ³n' -> 'estatus_de_recepción' (ORIGINAL):\n",
      list(remanentes_df.columns), "\n")

# =============================================================================
# FASE C: NORMALIZAR (REMOVER ACENTOS, minusculas, etc.) E IMPRIMIR
# =============================================================================
remanentes_df.columns = [
    remover_acentos(col).lower().strip().replace(" ", "_") for col in remanentes_df.columns
]
print("FASE C) Columnas tras normalizar (ORIGINAL):\n", list(remanentes_df.columns), "\n")

# =============================================================================
# FASE D: VERIFICACION FINAL (Si aparece "estatus_de_recepcian")
# =============================================================================
if "estatus_de_recepcian" in remanentes_df.columns:
    print("Detectamos la columna conflictiva 'estatus_de_recepcian' en el archivo original.")
    remanentes_df["estatus_de_recepcion"] = remanentes_df["estatus_de_recepcian"]
    remanentes_df.drop(columns=["estatus_de_recepcian"], inplace=True)
    print("Renombramos 'estatus_de_recepcian' -> 'estatus_de_recepcion'.")

print("FASE D) Columnas tras verificación final (ORIGINAL):\n", list(remanentes_df.columns), "\n")

# =============================================================================
# (Opcional) Eliminar registros según "eliminar_remanentes.xlsx"
# =============================================================================
if os.path.exists(archivo_eliminar):
    eliminar_df = pd.read_excel(archivo_eliminar, dtype=str)
    eliminar_df.columns = [
        remover_acentos(col).lower().strip().replace(" ", "_") for col in eliminar_df.columns
    ]

    if "folio_transferencia" in eliminar_df.columns and "folio_transferencia" in remanentes_df.columns:
        antes_eliminar = len(remanentes_df)
        remanentes_df = remanentes_df[~remanentes_df["folio_transferencia"].isin(eliminar_df["folio_transferencia"])]
        despues_eliminar = len(remanentes_df)
        print(f"✅ Se eliminaron {antes_eliminar - despues_eliminar} registros basados en 'eliminar_remanentes.xlsx'.")
    else:
        print("⚠️ No se encontró la columna 'folio_transferencia' en el archivo de eliminación.")
else:
    print("⚠️ No se encontró 'eliminar_remanentes.xlsx'. Se omite parte de eliminación.")

# =============================================================================
# LEER Y NORMALIZAR ARCHIVO CORREGIDO (SI EXISTE)
# =============================================================================
if os.path.exists(archivo_corregidos):
    corregidos_df = pd.read_csv(archivo_corregidos, dtype=str, encoding="utf-8")
    print("\nArchivo 'remanentes_corregido.csv' con columnas:\n", list(corregidos_df.columns))

    if "estatus_de_recepciÃ³n" in corregidos_df.columns:
        corregidos_df.rename(columns={"estatus_de_recepciÃ³n": "estatus_de_recepción"}, inplace=True)

    corregidos_df.columns = [
        remover_acentos(col).lower().strip().replace(" ", "_") for col in corregidos_df.columns
    ]

    if "estatus_de_recepcian" in corregidos_df.columns:
        print("Detectamos 'estatus_de_recepcian' en el archivo corregido.")
        corregidos_df["estatus_de_recepcion"] = corregidos_df["estatus_de_recepcian"]
        corregidos_df.drop(columns=["estatus_de_recepcian"], inplace=True)

    antes_corr = len(remanentes_df)
    remanentes_df = pd.concat([remanentes_df, corregidos_df], ignore_index=True)
    despues_corr = len(remanentes_df)
    print(f"✅ Se añadieron {despues_corr - antes_corr} registros de 'remanentes_corregido.csv'.")
else:
    print("\n⚠️ No se encontró 'remanentes_corregido.csv'. Se omite.")

# =============================================================================
# FECHAS: INTENTA %d/%m/%Y Y LUEGO %d/%m/%y
# =============================================================================
def convertir_fecha_dual(col_name):
    parse_col = f"{col_name}_parse"
    remanentes_df[parse_col] = pd.to_datetime(remanentes_df[col_name], format="%d/%m/%Y", errors="coerce")
    mask = remanentes_df[parse_col].isna() & remanentes_df[col_name].notna()
    remanentes_df.loc[mask, parse_col] = pd.to_datetime(remanentes_df.loc[mask, col_name], format="%d/%m/%y", errors="coerce")
    remanentes_df[col_name] = remanentes_df[parse_col].dt.date
    remanentes_df.drop(columns=[parse_col], inplace=True)

if "fecha_de_salida" in remanentes_df.columns:
    convertir_fecha_dual("fecha_de_salida")

if "fecha_de_llegada" in remanentes_df.columns:
    convertir_fecha_dual("fecha_de_llegada")

# =============================================================================
# ASIGNAR ID CEDA SEGUN ESTATUS
# =============================================================================
if "id_ceda_agricultura" not in remanentes_df.columns:
    remanentes_df["id_ceda_agricultura"] = None

if "estatus" in remanentes_df.columns:
    if "cdf_destino_final" in remanentes_df.columns:
        mask_entregada = remanentes_df["estatus"].str.lower() == "entregada"
        remanentes_df.loc[mask_entregada, "id_ceda_agricultura"] = remanentes_df["cdf_destino_final"]
    if "cdf_destino_original" in remanentes_df.columns:
        mask_no_entregada = remanentes_df["estatus"].str.lower() != "entregada"
        remanentes_df.loc[mask_no_entregada, "id_ceda_agricultura"] = remanentes_df["cdf_destino_original"]

print("\nAntes de Insertar, columnas definitivas:\n", list(remanentes_df.columns), "\n")

# =============================================================================
# DTYPE PARA LA INSERCION
# =============================================================================
dtype_sqlalchemy = {
    "folio_transferencia": Text,
    "cdf_origen": Text,
    "estatus": Text,
    "producto": Text,
    "abreviacion_producto": Text,
    "toneladas_iniciales": Numeric(10, 3),
    "toneladas_en_el_destino": Numeric(10, 3),
    "fecha_de_salida": Date,
    "cdf_destino_original": Text,
    "cdf_destino_final": Text,
    "fecha_de_llegada": Date,
    "estatus_de_recepcion": Text,
    "descripcion": Text,
    "destino_final": Text,
    "nombre_operador": Text,
    "telefono_operador": Text,
    "placas_transporte": Text,
    "tipo_transporte": Text,
    "bultos_iniciales": Integer,
    "numero_ticket_bascula": Text,
    "bultos_en_el_destino": Integer,
    "id_ceda_agricultura": Text
}

table_name = "remanentes"

print("⚠️ Revisar las columnas EXACTAS en 'remanentes_df' final y si coincide con 'dtype_sqlalchemy':")
for col in remanentes_df.columns:
    if col not in dtype_sqlalchemy:
        print(f" -> La columna '{col}' no está en dtype_sqlalchemy (posible causa de error).")

print(f"\nSe van a insertar {len(remanentes_df)} registros en '{table_name}'.")

# TRUNCATE la tabla
with engine.connect() as conn:
    conn.execute(text(f"TRUNCATE TABLE {table_name};"))
    conn.commit()

print("\n⬆️ Insertando en la tabla:", table_name)
remanentes_df.to_sql(
    name=table_name,
    con=engine,
    if_exists="append",
    index=False,
    dtype=dtype_sqlalchemy
)

print(f"✅ Se han sustituido los datos antiguos por los nuevos en la tabla '{table_name}' de la base '{DB_NAME}'.")
