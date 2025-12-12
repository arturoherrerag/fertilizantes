import os
import pandas as pd
import unicodedata
import re

# 📁 Ruta base
base_path = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_padrones"
output_csv = os.path.join(base_path, "derechohabientes_padrones_2025.csv")
duplicados_csv = os.path.join(base_path, "duplicados.csv")

# 🔍 Buscar todos los archivos .xlsx dentro de las subcarpetas
excel_files = []
for root, dirs, files in os.walk(base_path):
    for file in files:
        if file.endswith(".xlsx") and not file.startswith("~$"):
            excel_files.append(os.path.join(root, file))

print(f"📊 Se detectaron {len(excel_files)} archivos Excel.")

# 🧩 Unir todos los archivos en un solo DataFrame, agregando nombre de archivo
df_total = pd.DataFrame()
for archivo in excel_files:
    df = pd.read_excel(archivo, dtype=str)
    df["archivo_origen"] = os.path.basename(archivo)
    df_total = pd.concat([df_total, df], ignore_index=True)

# 🧼 Limpiar nombres de columnas
def limpiar_columna(col):
    col = unicodedata.normalize('NFKD', col)
    col = ''.join(c for c in col if not unicodedata.combining(c))
    col = col.strip().lower().replace(" ", "_")
    col = re.sub(r"[^a-z0-9_]", "", col)
    return col

df_total.columns = [limpiar_columna(c) for c in df_total.columns]

# 🔁 Verificar duplicados por acuse_estatal y guardar duplicados
if "acuse_estatal" not in df_total.columns:
    print("❌ ERROR: No se encontró la columna 'acuse_estatal'.")
    exit()

df_total["__orden__"] = df_total.index  # para conservar el último
duplicados = df_total[df_total.duplicated("acuse_estatal", keep=False)]
duplicados.drop(columns=["__orden__"]).to_csv(duplicados_csv, index=False, encoding="utf-8")
print(f"⚠️ Se encontraron {len(duplicados)} registros duplicados por 'acuse_estatal'. Se guardaron en:\n{duplicados_csv}")

# 🧹 Eliminar duplicados, conservar el último registro
df_total = df_total.sort_values("__orden__").drop(columns=["__orden__"])
df_total = df_total.drop_duplicates("acuse_estatal", keep="last")

# 💾 Guardar el archivo combinado final (sin duplicados)
df_total.to_csv(output_csv, index=False, encoding="utf-8")
print(f"✅ Archivo final sin duplicados guardado en:\n{output_csv}")

# 🧠 Inferencia de tipos de datos para SQL
def infer_sql_type(serie):
    datos = serie.dropna().astype(str).str.strip()

    if datos.empty:
        return "VARCHAR(1)"

    longitud_max = datos.str.len().max()
    if pd.isna(longitud_max):
        return "VARCHAR(1)"

    longitud_max = int(longitud_max)

    if datos.str.fullmatch(r"\d{4}-\d{2}-\d{2}").all():
        return "DATE"
    if datos.str.contains(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", regex=True).any():
        return f"VARCHAR({max(longitud_max, 1)})"
    try:
        numeros = pd.to_numeric(datos, errors="coerce")
        if numeros.dropna().apply(float.is_integer).all():
            return "INTEGER"
        elif not numeros.dropna().empty:
            return "FLOAT"
    except:
        pass
    return f"VARCHAR({max(longitud_max, 1)})"

# 🧾 Generar SQL CREATE TABLE
tipos_sql = {col: infer_sql_type(df_total[col]) for col in df_total.columns}
nombre_tabla = "derechohabientes_padrones_2025"
columnas_sql = [f'    "{col}" {tipo}' for col, tipo in tipos_sql.items()]
columnas_sql.append('    PRIMARY KEY ("acuse_estatal")')

sql = f"""CREATE TABLE {nombre_tabla} (
{',\n'.join(columnas_sql)}
);
"""

print("\n📄 Sentencia SQL para crear la tabla:")
print(sql)
