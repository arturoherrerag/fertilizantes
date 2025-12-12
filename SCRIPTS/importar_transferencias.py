import os
import glob
import unicodedata
import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import Text, Integer, Numeric, Date
from conexion import engine  # Usamos la conexión centralizada

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
archivo_transferencias = glob.glob(os.path.join(ruta_base, "*-FERTILIZANTES-TRANSFERENCIAS-NACIONAL-ANUAL_2025*.CSV"))[0]
archivo_eliminar = os.path.join(ruta_base, "eliminar_transferencias.xlsx")
archivo_corregidos = os.path.join(ruta_base, "transferencias_corregido.csv")

# =============================================================================
# LECTURA DEL CSV
# =============================================================================
transferencias_df = pd.read_csv(archivo_transferencias, dtype=str, encoding="utf-8")

# Renombrado especial
if "estatus_de_recepciÃ³n" in transferencias_df.columns:
    transferencias_df.rename(columns={"estatus_de_recepciÃ³n": "estatus_de_recepción"}, inplace=True)

# Normalizar nombres de columnas
transferencias_df.columns = [
    remover_acentos(col).lower().strip().replace(" ", "_") for col in transferencias_df.columns
]

# Verificación de errores comunes
if "estatus_de_recepcian" in transferencias_df.columns:
    transferencias_df["estatus_de_recepcion"] = transferencias_df["estatus_de_recepcian"]
    transferencias_df.drop(columns=["estatus_de_recepcian"], inplace=True)

# =============================================================================
# ELIMINAR REGISTROS
# =============================================================================
if os.path.exists(archivo_eliminar):
    eliminar_df = pd.read_excel(archivo_eliminar, dtype=str)
    eliminar_df.columns = [
        remover_acentos(col).lower().strip().replace(" ", "_") for col in eliminar_df.columns
    ]
    if "folio_transferencia" in eliminar_df.columns:
        antes = len(transferencias_df)
        transferencias_df = transferencias_df[~transferencias_df["folio_transferencia"].isin(eliminar_df["folio_transferencia"])]
        print(f"✅ Se eliminaron {antes - len(transferencias_df)} registros con 'eliminar_transferencias.xlsx'.")

# =============================================================================
# AGREGAR REGISTROS CORREGIDOS
# =============================================================================
if os.path.exists(archivo_corregidos):
    corregidos_df = pd.read_csv(archivo_corregidos, dtype=str, encoding="utf-8")
    corregidos_df.columns = [
        remover_acentos(col).lower().strip().replace(" ", "_") for col in corregidos_df.columns
    ]
    antes = len(transferencias_df)
    transferencias_df = pd.concat([transferencias_df, corregidos_df], ignore_index=True)
    print(f"✅ Se añadieron {len(transferencias_df) - antes} registros corregidos.")

# =============================================================================
# PARSEO DE FECHAS
# =============================================================================
for col in ["fecha_de_salida", "fecha_de_llegada"]:
    if col in transferencias_df.columns:
        transferencias_df[col] = pd.to_datetime(transferencias_df[col], errors="coerce").dt.date

# =============================================================================
# ASIGNAR 'id_ceda_agricultura'
# =============================================================================
if "id_ceda_agricultura" not in transferencias_df.columns:
    transferencias_df["id_ceda_agricultura"] = None

if "estatus" in transferencias_df.columns:
    if "cdf_destino_final" in transferencias_df.columns:
        mask_entregada = transferencias_df["estatus"].str.lower() == "entregada"
        transferencias_df.loc[mask_entregada, "id_ceda_agricultura"] = transferencias_df["cdf_destino_final"]

    if "cdf_destino_original" in transferencias_df.columns:
        mask_no_entregada = transferencias_df["estatus"].str.lower() != "entregada"
        transferencias_df.loc[mask_no_entregada, "id_ceda_agricultura"] = transferencias_df["cdf_destino_original"]

# =============================================================================
# DTYPE PARA LA INSERCIÓN
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
    "bultos_en_destino": Integer,
    "id_ceda_agricultura": Text
}

# =============================================================================
# INSERTAR A BASE DE DATOS
# =============================================================================
table_name = "transferencias"
print(f"\n🗑 Truncando tabla '{table_name}'...")
with engine.begin() as conn:
    conn.execute(text(f"TRUNCATE TABLE {table_name};"))

print(f"⬆️ Insertando {len(transferencias_df)} registros en '{table_name}'...")
transferencias_df.to_sql(
    name=table_name,
    con=engine,
    if_exists="append",
    index=False,
    dtype=dtype_sqlalchemy
)
print("✅ Proceso finalizado sin errores.")
