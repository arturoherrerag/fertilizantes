import pandas as pd, glob, os, unicodedata, re

# === Ajusta SOLO si tu ruta cambia ===
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"

# --- Localizar archivo ---
file_path = glob.glob(os.path.join(ruta_base, "*PEDIDOS DESGLOSE-NACIONAL-ANUAL*"))[0]

# --- Cargar CSV ---
df_pedidos = pd.read_csv(file_path, encoding="utf-8", delimiter=",", header=0)

# --- Normalizar nombres de columnas (tal como en tu script) ---
def normalizar_columna(col):
    col = col.strip().lower()
    col = ''.join(c for c in unicodedata.normalize('NFD', col) if unicodedata.category(c) != 'Mn')
    col = re.sub(r'[^a-z0-9]+', '_', col)
    col = re.sub(r'[_]+', '_', col).strip('_')
    return col
df_pedidos.columns = [normalizar_columna(c) for c in df_pedidos.columns]

# --- Mostrar resultados ---
print("Columnas:", df_pedidos.columns.tolist())
print("Primeros 10 valores de 'fecha':", df_pedidos["fecha"].head(10).tolist())