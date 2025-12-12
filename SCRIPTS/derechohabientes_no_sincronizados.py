import pandas as pd
from sqlalchemy import text
from conexion import engine

# -------------------------------
# 1. Leer datos desde el Excel
# -------------------------------
ruta_excel = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/CORRECCIONES DERECHOHABIENTES/derechohabientes_no_sincronizados_nacional.xlsx"
hoja = "Detalles depurado"

# Leer columnas: C (ID_CEDA_AGRICULTURA), D (Acuse_estatal), G (Fecha_Entrega)
df_excel = pd.read_excel(ruta_excel, sheet_name=hoja, usecols="C,D,G")
df_excel = df_excel.dropna(subset=["Acuse_estatal"])

# Normalizar
df_excel["Acuse_estatal"] = df_excel["Acuse_estatal"].astype(str).str.strip()
df_excel["ID_CEDA_AGRICULTURA"] = df_excel["ID_CEDA_AGRICULTURA"].astype(str).str.strip()
df_excel["Fecha_Entrega"] = pd.to_datetime(df_excel["Fecha_Entrega"], errors="coerce")

# Crear diccionarios para mapear datos
map_cdf = dict(zip(df_excel["Acuse_estatal"], df_excel["ID_CEDA_AGRICULTURA"]))
map_fecha = dict(zip(df_excel["Acuse_estatal"], df_excel["Fecha_Entrega"]))

# Armar lista de acuses
acuses = df_excel["Acuse_estatal"].unique().tolist()
acuses_str = ",".join(f"'{a}'" for a in acuses)

# -------------------------------
# 2. Consulta SQL con filtro
# -------------------------------
query = f"""
SELECT
    f.clave_estado_predio_capturada,
    f.estado_predio_capturada,
    f.clave_municipio_predio_capturada,
    f.municipio_predio_capturada,
    f.clave_localidad_predio AS clave_localidad_predio_capturada,
    f.localidad_predio AS localidad_predio_capturada,
    f.id_solicitud AS id_nu_solicitud,
    f.cdf_entrega,
    NULL AS id_cdf_entrega,
    f.acuse_estatal,
    f.curp_appmovil AS curp_solicitud,
    f.curp_renapo AS curp_renapo,
    f.curp_historica,
    f.primer_apellido AS sn_primer_apellido,
    f.segundo_apellido AS sn_segundo_apellido,
    f.nombre AS ln_nombre,
    f.es_pob_indigena,
    f.cultivo,
    d.dap_ton AS ton_dap_entregada,
    d.urea_ton AS ton_urea_entregada,
    f.fecha_entrega,
    f.id_persona AS folio_persona,
    f.cuadernillo,
    f.nombre_ddr AS nombre_ddr,
    f.clave_ddr AS clave_ddr,
    f.nombre_cader_ventanilla,
    f.clave_cader_ventanilla,
    ROUND(COALESCE(d.dap_ton, 0) / 0.025)::INT AS dap_anio_actual,
    ROUND(COALESCE(d.urea_ton, 0) / 0.025)::INT AS urea_anio_actual,
    0 AS dap_remanente,
    0 AS urea_remanente,
    f.superficie_apoyada,
    f.id_persona AS id_beneficiarios_fertilizantes,
    NULL AS fecha_creacion,
    NULL AS id_nu_taxonomia
FROM full_derechohabientes_2025 f
LEFT JOIN derechohabientes_padrones_2025 d
    ON f.acuse_estatal = d.acuse_estatal
WHERE f.acuse_estatal IN ({acuses_str})
"""

# -------------------------------
# 3. Ejecutar consulta
# -------------------------------
df = pd.read_sql_query(text(query), engine)

# 🔄 Reemplazar datos con los del Excel usando map
df["cdf_entrega"] = df["acuse_estatal"].map(map_cdf)
df["fecha_entrega"] = df["acuse_estatal"].map(map_fecha)

# -------------------------------
# 4. Exportar
# -------------------------------
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/CORRECCIONES DERECHOHABIENTES/derechohabientes_no_sincronizados_importar.csv"
df.to_csv(ruta_salida, index=False, encoding="utf-8-sig")

print(f"✅ Exportado {len(df)} registros a:\n{ruta_salida}")