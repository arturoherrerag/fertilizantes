import pandas as pd
from conexion import engine  # Conexión centralizada con SQLAlchemy

estados = ["GUERRERO", "TLAXCALA", "MORELOS", "MICHOACÁN", "DURANGO", "CHIAPAS"]
vista_sql = "abasto_y_remanente_x_estado_2025"
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS"

for estado in estados:
    try:
        print(f"📤 Exportando '{vista_sql}' para {estado}...")

        query = f"SELECT * FROM {vista_sql} WHERE estado = '{estado}';"
        df = pd.read_sql(query, engine)

        ruta_salida = f"{ruta_base}/abasto_{estado.lower()}.csv"
        df.to_csv(ruta_salida, index=False, encoding="utf-8-sig")

        print(f"✅ Exportado correctamente: {ruta_salida}")

    except Exception as e:
        print(f"❌ Error al exportar {estado}: {e}")
