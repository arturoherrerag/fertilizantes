import pandas as pd
import unicodedata
from conexion import engine  # Conexión centralizada con SQLAlchemy

# Lista de estados que deseas exportar
estados = ["GUERRERO", "MICHOACÁN", "MORELOS", "TLAXCALA", "DURANGO", "CHIAPAS"]
vista_sql = "entregas_diarias_x_estado_2025"
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS"

def normalizar_nombre(nombre):
    # Elimina acentos y convierte a minúsculas
    return unicodedata.normalize('NFKD', nombre).encode('ASCII', 'ignore').decode().lower()

for estado in estados:
    try:
        print(f"📤 Exportando entregas para {estado}...")

        # Consulta con filtro por estado
        query = f"""
        SELECT * FROM {vista_sql}
        WHERE estado = '{estado}'
        ORDER BY fecha;
        """

        # Leer y exportar
        df = pd.read_sql(query, engine)
        nombre_archivo = f"entregas_{normalizar_nombre(estado)}.csv"
        ruta_salida = f"{ruta_base}/{nombre_archivo}"
        df.to_csv(ruta_salida, index=False, encoding="utf-8-sig")

        print(f"✅ Exportado correctamente: {ruta_salida}")

    except Exception as e:
        print(f"❌ Error al exportar {estado}: {e}")
