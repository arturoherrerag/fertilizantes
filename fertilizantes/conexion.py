from sqlalchemy import create_engine
import psycopg2

"""
Capa de conexión para el sistema de Fertilizantes.

- 2025: base de datos histórica (BD actual del sistema web)
- 2026: base de datos de operación 2026

Por compatibilidad:
- DB_NAME, engine, psycopg_conn siguen apuntando a la BD 2025
  (como hasta ahora).
- Para 2026 usaremos helpers: get_engine_for_year(2026), get_psycopg_conn_for_year(2026).
"""

# Configuración base (usuario/host/puerto compartidos)
DB_USER = "postgres"
DB_PASSWORD = "Art4125r0"
DB_HOST = "localhost"
DB_PORT = "5432"

# Nombres de las bases por año
DB_NAME_2025 = "fertilizantes"       # BD actual del sistema (histórica 2025)
DB_NAME_2026 = "fertilizantes_2026"  # BD nueva para operación 2026


def _make_sqlalchemy_url(db_name: str) -> str:
    """Construye la URL de SQLAlchemy para una BD dada."""
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{db_name}"


def get_engine_for_year(anio: int):
    """
    Devuelve un engine de SQLAlchemy para el año indicado.
    - 2025 → BD_NAME_2025
    - 2026 → BD_NAME_2026
    Cualquier otro año, por ahora, cae en 2025.
    """
    if anio == 2026:
        db_name = DB_NAME_2026
    else:
        db_name = DB_NAME_2025

    return create_engine(_make_sqlalchemy_url(db_name))


def get_psycopg_conn_for_year(anio: int):
    """
    Devuelve una conexión psycopg2 para el año indicado.
    No se mantiene abierta de forma global; la idea es usarla
    con context managers o cerrarla explícitamente después de usarla.
    """
    if anio == 2026:
        db_name = DB_NAME_2026
    else:
        db_name = DB_NAME_2025

    return psycopg2.connect(
        dbname=db_name,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


# ===========================
# Compatibilidad con el código actual (2025)
# ===========================

# Por compatibilidad, dejamos estas variables apuntando a la BD 2025
DB_NAME = DB_NAME_2025

# Crear conexión SQLAlchemy para pandas/to_sql (BD 2025)
engine = create_engine(_make_sqlalchemy_url(DB_NAME_2025))

# Crear conexión psycopg2 para COPY (BD 2025)
psycopg_conn = psycopg2.connect(
    dbname=DB_NAME_2025,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
)