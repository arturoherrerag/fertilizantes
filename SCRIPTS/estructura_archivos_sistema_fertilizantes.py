import os

# Ruta base del proyecto
base_dir = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/dashboard_web"
# Archivo de salida en el escritorio
output_file = "/Users/Arturo/Desktop/estructura_y_contenido_html.txt"

with open(output_file, 'w', encoding='utf-8') as out:
    out.write("📁 ESTRUCTURA DE ARCHIVOS HTML Y SU CONTENIDO\n\n")
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".html"):
                filepath = os.path.join(root, file)
                relative_path = os.path.relpath(filepath, base_dir)
                out.write(f"\n🔹 Ruta: {relative_path}\n")
                out.write(f"📄 Archivo: {file}\n")
                out.write("=" * 80 + "\n")
                try:
                    with open(filepath, 'r', encoding='utf-8') as html_file:
                        content = html_file.read()
                        out.write(content + "\n")
                except Exception as e:
                    out.write(f"[ERROR AL LEER ARCHIVO]: {e}\n")
                out.write("=" * 80 + "\n\n")

print(f"✅ Archivo generado: {output_file}")
