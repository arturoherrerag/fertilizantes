import pandas as pd
from datetime import datetime
from conexion import engine  # Usa la conexión centralizada

# Leer datos desde la vista
consulta = "SELECT * FROM reporte_derechohabientes_bienestar_td"
df = pd.read_sql(consulta, engine)

# Obtener fecha en formato mmddaaaa
fecha_actual = datetime.now().strftime("%d%m%Y")  # Resultado: "18072025"

# Definir ruta base (sin extensión)
ruta_base = f"/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/Fertilizantes - seguimiento beneficiarios - {fecha_actual}"

# Calcular cortes para 5 partes equilibradas
total = len(df)
base = total // 5
resto = total % 5

# Distribuir cortes con el resto para equilibrio
cortes = [0]
for i in range(5):
    incremento = base + (1 if i < resto else 0)
    cortes.append(cortes[-1] + incremento)

# Exportar cada parte a XLSX
for i in range(5):
    parte_df = df.iloc[cortes[i]:cortes[i+1]]
    nombre_archivo = f"{ruta_base}_parte{i+1}.csv"
    # parte_df.to_excel(nombre_archivo, index=False, engine='openpyxl')
    # Si prefieres CSV, comenta la línea anterior y descomenta esta:
    parte_df.to_csv(nombre_archivo.replace('.xlsx', '.csv'), index=False, encoding='utf-8-sig')

# Mostrar resumen
print("✅ Archivos exportados exitosamente:")
for i in range(5):
    print(f"  📁 Parte {i+1}: {ruta_base}_parte{i+1}.csv ({cortes[i+1] - cortes[i]} registros)")
