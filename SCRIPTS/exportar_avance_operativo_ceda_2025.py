import pandas as pd
from conexion import engine  # Usa la conexión centralizada

# Leer datos desde la vista entregas_diarias_2025
consulta = "SELECT * FROM avance_operativo_ceda_2025"
df = pd.read_sql(consulta, engine)

# Exportar a CSV
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/avance_operativo_ceda_2025.csv"
df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print(f"✅ Archivo exportado exitosamente a: {ruta_salida}")
