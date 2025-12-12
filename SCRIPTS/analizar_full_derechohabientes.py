import os
import pandas as pd
import unicodedata
import re

# Ruta base
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/U_TEMPORAL/"
archivos = [
    "1051-FERTILIZANTES-FULL-NACIONAL-SEGUNDO CORTE_2025-05-09 18_26_46_.csv",
    "1051-FERTILIZANTES-FULL-NACIONAL-PRIMER CORTE_2025-05-09 16_30_38_.csv",
    "1051-FERTILIZANTES-FULL-NACIONAL-TERCER CORTE_2025-05-10 12_44_16_.csv",
    "1051-FERTILIZANTES-FULL-NACIONAL-CUARTO CORTE_2025-05-10 16_56_37_.csv",
    "1051-FERTILIZANTES-FULL-NACIONAL-QUINTO CORTE_2025-05-10 17_01_10_.csv",
    "1051-FERTILIZANTES-FULL-NACIONAL-SEXTO CORTE_2025-05-10 17_05_41_.csv",
    "1051-FERTILIZANTES-FULL-SINALOA-ANUAL_2025-05-10 17_06_24_.csv",
]

# Limpieza de nombres de columnas
def limpiar_columna(nombre):
    nombre = unicodedata.normalize('NFKD', nombre)
    nombre = ''.join(c for c in nombre if not unicodedata.combining(c))
    nombre = nombre.lower().replace(" ", "_")
    nombre = re.sub(r"[^a-z0-9_]", "", nombre)  # eliminar símbolos raros
    return nombre

# Leer archivos y concatenar
df_total = pd.DataFrame()
for archivo in archivos:
    path = os.path.join(ruta_base, archivo)
    df = pd.read_csv(path, dtype=str, encoding="utf-8")
    if "Estatus Solicitud" in df.columns:
        df = df.rename(columns={"Estatus Solicitud": "estatus_solicitud_pago"})
    df_total = pd.concat([df_total, df], ignore_index=True)

# Normalizar columnas
df_total.columns = [limpiar_columna(col) for col in df_total.columns]

# Función para detectar tipo SQL
def detectar_tipo(columna, nombre):
    datos = columna.dropna().astype(str).str.strip()
    longitud_max = int(datos.str.len().max())

    campos_texto = [
        "nombre", "apellido", "estado", "municipio", "localidad", "sexo",
        "cultivo", "estatus", "regimen", "etapa", "cader", "ddr", "cuadernillo"
    ]
    if any(p in nombre for p in campos_texto):
        return f"VARCHAR({max(longitud_max, 10)})"
    
    if "curp" in nombre:
        return "VARCHAR(18)"
    if "clave" in nombre or "cdf_entrega" in nombre or "acuse" in nombre:
        return f"VARCHAR({max(longitud_max, 10)})"
    
    if datos.str.fullmatch(r"\d{4}-\d{2}-\d{2}").all():
        return "DATE"

    contiene_letras = datos.str.contains(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", regex=True).any()
    if contiene_letras:
        return f"VARCHAR({longitud_max})"

    try:
        numeros = pd.to_numeric(datos, errors='coerce')
        if numeros.dropna().apply(float.is_integer).all():
            return "INTEGER"
        elif not numeros.dropna().empty:
            return "FLOAT"
    except:
        pass

    return f"VARCHAR({longitud_max})"

# Detectar tipo por columna
tipos_sql = {col: detectar_tipo(df_total[col], col) for col in df_total.columns}

# Generar SQL
nombre_tabla = "full_derechohabientes_2025"
columnas_sql = [f'    "{col}" {tipo}' for col, tipo in tipos_sql.items()]
sql_create = f"""CREATE TABLE {nombre_tabla} (
{',\n'.join(columnas_sql)},
    PRIMARY KEY ("acuse_estatal")
);
"""

# Relaciones e índices
sql_relaciones = f"""
-- Relaciones
ALTER TABLE {nombre_tabla}
    ADD CONSTRAINT fk_derechohabientes FOREIGN KEY ("acuse_estatal") REFERENCES derechohabientes("acuse_estatal"),
    ADD CONSTRAINT fk_red_distribucion FOREIGN KEY ("cdf_entrega") REFERENCES red_distribucion("id_ceda_agricultura");

-- Índices
CREATE INDEX idx_curp_appmovil ON {nombre_tabla}("curp_appmovil");
CREATE INDEX idx_curp_renapo ON {nombre_tabla}("curp_renapo");
CREATE INDEX idx_acuse_estatal ON {nombre_tabla}("acuse_estatal");
CREATE INDEX idx_cdf_entrega ON {nombre_tabla}("cdf_entrega");
"""

# Relación opcional con dim_fecha
sql_fecha = ""
if "fecha_entrega" in df_total.columns:
    sql_fecha = f"""
-- Relación opcional con dim_fecha
-- ALTER TABLE {nombre_tabla}
--     ADD CONSTRAINT fk_fecha_entrega FOREIGN KEY (fecha_entrega) REFERENCES dim_fecha(fecha);
"""
else:
    sql_fecha = "-- No se detectó columna 'fecha_entrega'."

# Guardar SQL final
ruta_sql = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS/sql_crear_tabla_full_derechohabientes_2025.sql"
with open(ruta_sql, "w", encoding="utf-8") as f:
    f.write("-- SQL generado automáticamente\n\n")
    f.write(sql_create + "\n")
    f.write(sql_relaciones + "\n")
    f.write(sql_fecha + "\n")

print(f"✅ SQL generado y guardado en: {ruta_sql}")
