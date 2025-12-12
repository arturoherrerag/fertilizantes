from pathlib import Path
from pypdf import PdfReader, PdfWriter
import re
from collections import defaultdict
import os
from tqdm import tqdm

# Rutas
PDF_INPUT = Path("/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/MANIFESTACION_PROTESTA.pdf")
OUTPUT_DIR = PDF_INPUT.parent / "manifestaciones_por_estado"
OUTPUT_DIR.mkdir(exist_ok=True)

# Expresión regular para detectar el estado antes de la fecha
patron_estado = re.compile(r"([A-ZÁÉÍÓÚÑa-záéíóúñ ]+?) a 01 de abril de 2025")

# Lectura del PDF
reader = PdfReader(str(PDF_INPUT))
writers = defaultdict(PdfWriter)

for i, page in enumerate(tqdm(reader.pages, desc="📄 Procesando páginas")):
    texto = page.extract_text() or ""
    match = patron_estado.search(texto)
    estado = match.group(1).strip().title().replace(" ", "_") if match else "estado_desconocido"
    writers[estado].add_page(page)

# Guardar archivos por estado
for estado, writer in writers.items():
    archivo = OUTPUT_DIR / f"{estado}_protestas.pdf"
    with open(archivo, "wb") as f:
        writer.write(f)
    print(f"✅ {estado}: {len(writer.pages)} páginas → {archivo}")
