from conexion import engine
from sqlalchemy import text  # solo si necesitas ejecutar SQL directamente

# Listado de tablas en orden para limpiar
tablas_ordenadas = [
    "derechohabientes",
    "fletes",
    "transferencias",
    "remanentes",
    "pedidos_desglosado",
    "pedidos_sigap",
    "incidentes",
    "red_distribucion"  # Esta debe ir al final
]

try:
    with engine.begin() as conn:
        print("🚨 Iniciando limpieza de tablas...")
        for tabla in tablas_ordenadas:
            print(f"🧹 Limpiando: {tabla}...")
            conn.execute(text(f"TRUNCATE {tabla} CASCADE;"))
        print("✅ Todas las tablas fueron limpiadas correctamente.")

except Exception as e:
    print(f"❌ Error durante la limpieza: {e}")
