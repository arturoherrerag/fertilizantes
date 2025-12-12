import pandas as pd
from sqlalchemy import text
import os
import glob
import unicodedata
import re
from conexion import engine, DB_NAME

# Ruta base
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"
archivo_eliminar = os.path.join(ruta_base, "eliminar_pedidos_desglosado.csv")

# Buscar archivo con la máscara "*-PEDIDOS DESGLOSE-NACIONAL-ANUAL_*"
file_list = glob.glob(os.path.join(ruta_base, "*PEDIDOS DESGLOSE-NACIONAL-ANUAL*"))
if not file_list:
    print("❌ Error: No se encontró ningún archivo que coincida con la máscara.")
    exit()
file_path = file_list[0]

# Cargar CSV principal
df_pedidos = pd.read_csv(file_path, encoding="utf-8", delimiter=",", header=0)

# Normalizar nombres de columnas
def normalizar_columna(col):
    col = col.strip().lower()
    col = ''.join(c for c in unicodedata.normalize('NFD', col) if unicodedata.category(c) != 'Mn')
    col = re.sub(r'[^a-z0-9]+', '_', col)
    col = re.sub(r'[_]+', '_', col).strip('_')
    return col

df_pedidos.columns = [normalizar_columna(c) for c in df_pedidos.columns]

# Reemplazar valores no válidos
df_pedidos.replace(["N/A", "NA", "n/a", "-"], None, inplace=True)

# Convertir columnas numéricas
for col in ["dap", "urea"]:
    if col in df_pedidos.columns:
        df_pedidos[col] = pd.to_numeric(df_pedidos[col], errors="coerce").round(3)

# Insertar ID consecutivo
df_pedidos.insert(0, "id_pedido", range(1, len(df_pedidos) + 1))

# Filtrar por 'AUTORIZADO'
if "estatus_pedido_detalle" in df_pedidos.columns:
    df_pedidos = df_pedidos[df_pedidos["estatus_pedido_detalle"] == "AUTORIZADO"]
else:
    print("⚠️ No se encontró la columna 'estatus_pedido_detalle'. No se aplicará el filtro.")

# Eliminar registros según archivo externo
if os.path.exists(archivo_eliminar):
    df_eliminar = pd.read_csv(archivo_eliminar, dtype=str, encoding="utf-8")
    df_eliminar.columns = [normalizar_columna(c) for c in df_eliminar.columns]

    if "id_ceda_agricultura" in df_eliminar.columns and "folio_cdf" in df_pedidos.columns:
        antes = len(df_pedidos)
        df_pedidos = df_pedidos[~df_pedidos["folio_cdf"].isin(df_eliminar["id_ceda_agricultura"])]
        print(f"✅ Se eliminaron {antes - len(df_pedidos)} registros basados en 'eliminar_pedidos_desglosado.csv'.")
    else:
        print("⚠️ No se encontró la columna requerida en alguno de los archivos.")
else:
    print("⚠️ No se encontró el archivo 'eliminar_pedidos_desglosado.csv'. No se eliminaron registros.")

# Verificación rápida
print("✅ Columnas tras limpieza:", df_pedidos.columns.tolist())
print("🔍 Vista previa:")
print(df_pedidos[["id_pedido", "folio_cdf", "dap", "urea"]].head(10))

# Importación a PostgreSQL
try:
    with engine.begin() as conn:
        print("🧹 Eliminando registros anteriores de 'pedidos_desglosado'...")
        conn.execute(text("DELETE FROM pedidos_desglosado;"))
        print("⬆️ Insertando nuevos registros...")
        df_pedidos.to_sql("pedidos_desglosado", conn, if_exists="append", index=False)

    print(f"✅ Se han sustituido los datos antiguos por los nuevos en la tabla 'pedidos_desglosado' de la base '{DB_NAME}'.")
except Exception as e:
    print(f"❌ Error al importar los datos: {e}")
