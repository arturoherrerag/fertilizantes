import os
import time
import subprocess

# Ruta a la carpeta de scripts
RUTA_SCRIPTS = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS"

# Scripts de truncado (debe ir primero)
scripts_de_limpieza = [
    "truncar_tablas.py"
]

# Scripts de importación en orden
scripts_importacion = [
    "importar_red.py",
    "importar_pedidos_desglosado.py",
    "importar_pedidos_sigap.py",
    "importar_fletes.py",
    "importar_transferencias.py",
    "importar_remanentes.py",
    "importar_incidencias.py",
    "importar_derechohabientes.py"
]

# Scripts de exportación
scripts_exportacion = [
    "exportar_entregas_td.py",
    "exportar_fletes_conteo_td.py",
    "exportar_fletes_fechas_td.py",
    "exportar_fletes_ton_td.py",
    "exportar_incidencias_td.py",
    "exportar_pedidos_sigap_td.py",
    "exportar_pedidos_td.py",
    "exportar_red_td.py",
    "exportar_remanentes_td.py",
    "exportar_transferencias_td.py",
    "exportar_abasto_x_estado_2025.py",
    "exportar_avances_2025_td.py",
    "exportar_entregas_diarias_2025.py",
    "exportar_envios_diarios_2025.py",
    "exportar_resumen_derechohabientes_apoyados_x_municipio.py",
    "exportar_abasto_y_remanente_x_dia_2025.py",
    "exportar_pedidos_por_dia_mas_remanentes.py",
    "exportar_abasto_y_remanente_por_dia_sin_transito_2025.py",
    "exportar_entregas_por_estado_y_genero.py",
    "exportar_entregas_semanales_2025.py",
    "exportar_pedidos_por_dia.py",
    "exportar_reporte_derechohabientes_bienestar_td.py",
    "grafico_Abasto Proyectado vs Real vs Pedido vs Entregado.py",
    "grafico_entregas_semanales_2025.py",
    "grafico_envios_diarios.py",
    "grafico_recibido_vs_entregado.py",
    "exportar_reporte_fletes_bienestar.py",
    "exportar_entregas_x_estado_no_ceda_y_fecha_2025.py",
    "exportar_avance_operativo_ceda_2025.py"
]

# Combinación de scripts: limpieza + importación
scripts_preparacion = scripts_de_limpieza + scripts_importacion

# ------------------------------------------------------------------------
print("🚀 Iniciando procedimiento completo de actualización...\n")
inicio = time.time()

errores = []

# Ejecutar scripts de limpieza e importación
for script in scripts_preparacion:
    ruta_script = os.path.join(RUTA_SCRIPTS, script)
    print(f"▶️ Ejecutando: {script}...")
    try:
        resultado = subprocess.run(
            ["/Users/Arturo/AGRICULTURA/FERTILIZANTES/ENTORNO/env/bin/python", ruta_script],
            capture_output=True,
            text=True
        )
        print(resultado.stdout)
        if resultado.stderr:
            print(f"⚠️ Advertencia en {script}:\n{resultado.stderr}")
            errores.append((script, resultado.stderr.strip()))
        print(f"✅ Finalizado: {script}\n")
    except Exception as e:
        error_msg = f"{script}: {str(e)}"
        print(f"❌ Error al ejecutar {error_msg}")
        errores.append((script, str(e)))

# -----------------------------------------------------------
# CONEXIÓN A LA BASE DE DATOS Y ACTUALIZACIÓN DE DATOS
# -----------------------------------------------------------
from sqlalchemy import create_engine, text

print("🔁 Conectando a la base de datos...\n")
engine = create_engine("postgresql://postgres:Art4125r0@localhost:5432/fertilizantes")

# Actualizar superficie_apoyada para CHIAPAS y OAXACA
try:
    print("🛠️ Actualizando superficie_apoyada = 1 para CHIAPAS y OAXACA...\n")
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE derechohabientes
            SET superficie_apoyada = 1
            WHERE estado_predio_capturada IN ('CHIAPAS', 'OAXACA')
              AND superficie_apoyada IS DISTINCT FROM 1;
        """))
        print(f"✅ Registros actualizados: {result.rowcount}\n")
except Exception as e:
    print(f"❌ Error al actualizar superficie_apoyada: {e}")
    errores.append(("UPDATE superficie_apoyada", str(e)))

# -----------------------------------------------------------
# REFRESCAR VISTAS MATERIALIZADAS
# -----------------------------------------------------------
vistas_materializadas = [
    "avance_operativo_ceda_2025",
    "entregas_diarias_x_estado_ceda_2025",
    "entregas_x_estado_no_ceda_y_fecha_2025",
    "inventario_acumulado_x_ceda_diario_2025",
    "inventario_remanente_x_ceda_diario_2025",
    "inventario_remanente_x_ceda_2025",
]

try:
    print("🔁 Refrescando vistas materializadas...\n")
    with engine.begin() as conn:
        for vista in vistas_materializadas:
            print(f"🔁 Refrescando vista materializada {vista}...")
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {vista};"))
            print(f"✅ Vista {vista} actualizada correctamente.\n")
except Exception as e:
    print(f"❌ Error al refrescar vistas materializadas: {e}")
    errores.append(("REFRESH vistas materializadas", str(e)))

# Ejecutar scripts de exportación y gráficos
for script in scripts_exportacion:
    ruta_script = os.path.join(RUTA_SCRIPTS, script)
    print(f"▶️ Ejecutando: {script}...")
    try:
        resultado = subprocess.run(
            ["/Users/Arturo/AGRICULTURA/FERTILIZANTES/ENTORNO/env/bin/python", ruta_script],
            capture_output=True,
            text=True
        )
        print(resultado.stdout)
        if resultado.stderr:
            print(f"⚠️ Advertencia en {script}:\n{resultado.stderr}")
            errores.append((script, resultado.stderr.strip()))
        print(f"✅ Finalizado: {script}\n")
    except Exception as e:
        error_msg = f"{script}: {str(e)}"
        print(f"❌ Error al ejecutar {error_msg}")
        errores.append((script, str(e)))

fin = time.time()

# -----------------------------------------------------------
# RESUMEN FINAL
# -----------------------------------------------------------
print("\n📋 Resumen final de la ejecución:\n")

if errores:
    print("❌ Se detectaron errores en los siguientes scripts:")
    for i, (script, msg) in enumerate(errores, 1):
        print(f"{i}. {script} ➤ {msg}")
else:
    print("✅ Todos los scripts se ejecutaron correctamente.")

print(f"\n⏱️ Tiempo total de ejecución: {round(fin - inicio, 2)} segundos.")
