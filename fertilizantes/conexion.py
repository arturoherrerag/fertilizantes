from sqlalchemy import create_engine
import psycopg2

# Configuración de la base de datos
DB_USER = "postgres"
DB_PASSWORD = "Art4125r0"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "fertilizantes"

# Crear conexión SQLAlchemy para pandas/to_sql
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Crear conexión psycopg2 para COPY
psycopg_conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)