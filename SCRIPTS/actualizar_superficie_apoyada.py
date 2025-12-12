# actualizar_superficie_apoyada.py

from sqlalchemy import text
from conexion import engine

def actualizar_superficie():
    print("🛠️ Actualizando superficie_apoyada = 1 para CHIAPAS y OAXACA...\n")
    try:
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

if __name__ == "__main__":
    actualizar_superficie()
