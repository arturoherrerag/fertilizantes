import psycopg2

# Configuración de la base de datos
DB_HOST = "localhost"
DB_NAME = "fertilizantes"
DB_USER = "postgres"
DB_PASSWORD = "Art4125r0"
DB_PORT = "5432"  # Puerto por defecto de PostgreSQL

try:
    # Intentar conexión a la base de datos
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    print("✅ Conexión exitosa a PostgreSQL")
    conn.close()
except Exception as e:
    print(f"❌ Error en la conexión: {e}")
