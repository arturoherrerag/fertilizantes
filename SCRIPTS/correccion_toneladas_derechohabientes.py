#!/usr/bin/env python3
import subprocess
from sqlalchemy import text
from conexion import engine, DB_NAME

# Paso 1: Corregir los datos calculando toneladas entregadas
update_query = """
UPDATE derechohabientes
SET 
    ton_dap_entregada = ROUND(((dap_anio_actual + dap_remanente) * 0.025)::numeric, 3),
    ton_urea_entregada = ROUND(((urea_anio_actual + urea_remanente) * 0.025)::numeric, 3)
WHERE ton_dap_entregada = 0 AND ton_urea_entregada = 0;
"""

print(f"🛠️ Corrigiendo valores nulos en 'ton_dap_entregada' y 'ton_urea_entregada' de la tabla 'derechohabientes' en la base '{DB_NAME}'...")

with engine.begin() as conn:
    result = conn.execute(text(update_query))
    print(f"✅ Se actualizaron {result.rowcount} registros.")

# Paso 2: Actualizar vistas materializadas
vistas_materializadas = [
    "avance_operativo_ceda_2025",
    "entregas_diarias_x_estado_ceda_2025",
    "entregas_x_estado_no_ceda_y_fecha_2025",
    "inventario_acumulado_x_ceda_diario_2025",
    "inventario_remanente_x_ceda_diario_2025",
    "inventario_remanente_x_ceda_2025",
]

print("🔄 Actualizando vistas materializadas...")
with engine.begin() as conn:
    for vista in vistas_materializadas:
        print(f"🔁 REFRESH MATERIALIZED VIEW {vista}...")
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {vista};"))
print("✅ Vistas actualizadas.")

# Paso 3: Ejecutar scripts de exportación y gráficos
scripts_exportacion = [
    "exportar_entregas_td.py",
    "exportar_remanentes_td.py",
    "exportar_avances_2025_td.py",
    "exportar_entregas_diarias_2025.py",
    "exportar_resumen_derechohabientes_apoyados_x_municipio.py",
    "exportar_abasto_y_remanente_por_dia_sin_transito_2025.py",
    "exportar_entregas_por_estado_y_genero.py",
    "exportar_entregas_semanales_2025.py",
    "exportar_reporte_derechohabientes_bienestar_td.py",
    "grafico_Abasto Proyectado vs Real vs Pedido vs Entregado.py",
    "grafico_entregas_semanales_2025.py",
    "grafico_entregas_diarias.py",
    "grafico_recibido_vs_entregado.py",
    "exportar_entregas_x_estado_no_ceda_y_fecha_2025.py",
    "exportar_avance_operativo_ceda_2025.py"
]

print("📤 Ejecutando scripts de exportación y generación de gráficos...")
for script in scripts_exportacion:
    ruta_script = f"/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS/{script}"
    print(f"▶️ Ejecutando: {script}")
    result = subprocess.run(["python3", ruta_script], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"✅ {script} ejecutado correctamente.")
    else:
        print(f"❌ Error en {script}:\n{result.stderr}")

print("🎉 Proceso completo.")
