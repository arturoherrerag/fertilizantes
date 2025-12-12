from sqlalchemy import create_engine, text

# Configuración de conexión
DB_USER = "postgres"
DB_PASSWORD = "Art4125r0"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "fertilizantes"

# Crear engine de conexión
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

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
