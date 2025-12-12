#!/usr/bin/env python3
'''
fast_docx_pandoc_pdf.py
-----------------------------------------
Conversión ultra‑rápida de un DOCX (contratos.docx)
a PDF utilizando Pandoc + WeasyPrint.

Requisitos previos (una sola vez):
  brew install pandoc weasyprint

Cómo usar:
  python fast_docx_pandoc_pdf.py
     → crea contratos.pdf junto al DOCX original
'''
import subprocess
from pathlib import Path
import sys
import shlex

DOCX = Path('/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/contratos.docx')
HTML = DOCX.with_suffix('.html')
PDF  = DOCX.with_suffix('.pdf')

def run(cmd: str):
    '''Ejecuta comando shell mostrando salida en vivo.'''
    print(f"⚙️  {cmd}")
    result = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if result.returncode != 0:
        sys.exit(f"💥 Error ({result.returncode}) al ejecutar: {cmd}")

def main():
    if not DOCX.exists():
        sys.exit(f"❌ No se encontró {DOCX}")

    # 1. DOCX → HTML vía Pandoc
    run(f"pandoc {DOCX} -o {HTML} --from=docx --to=html")

    # 2. HTML → PDF vía WeasyPrint
    run(f"weasyprint {HTML} {PDF}")

    # 3. Opcional: eliminar HTML temporal (comenta si lo quieres conservar)
    HTML.unlink(missing_ok=True)

    print(f"✅ PDF generado en: {PDF}")

if __name__ == '__main__':
    main()
