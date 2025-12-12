#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Genera un 'Context Pack' con todo lo necesario para que un proyecto nuevo
de ChatGPT conozca tu sistema sin partir de cero.

Crea una carpeta CONTEXT_PACK_YYYY-MM-DD con:
- repo_tree.txt                   (árbol de archivos)
- estructura_y_contenido_html.txt (todas las plantillas + contenido)
- endpoints_map.md                (urls ↔ views)
- estructura_actual_bd.csv        (esquema BD real via information_schema)
- vistas_fertilizantes.sql        (si existe, copia)
- acumulado_queries.txt           (si existe, copia)
- acumulado_scripts.txt           (contenido de /SCRIPTS/*.py)

Autor: Arturo (Sistema Fertilizantes)
"""

from pathlib import Path
import re
import shutil
import datetime as dt
import pandas as pd
from sqlalchemy import text

# === RUTAS PRINCIPALES ===
ROOT = Path("/Users/Arturo/AGRICULTURA/FERTILIZANTES/dashboard_web")
SCRIPTS_DIR = Path("/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS")
OUT = Path("/Users/Arturo/AGRICULTURA/FERTILIZANTES") / f"CONTEXT_PACK_{dt.date.today().isoformat()}"
OUT.mkdir(parents=True, exist_ok=True)

# Usa tu conexión ya estandarizada
from conexion import engine  # noqa: E402

def write_repo_tree():
    tree_file = OUT / "repo_tree.txt"
    lines = []
    for p in ROOT.rglob("*"):
        # Ignorar cachés y binarios pesados
        if any(part in p.parts for part in ["__pycache__", ".git", ".DS_Store", "env", "ENTORNO"]):
            continue
        rel = p.relative_to(ROOT)
        kind = "📁" if p.is_dir() else "📄"
        lines.append(f"{kind} {rel}")
    tree_file.write_text("\n".join(sorted(lines)), encoding="utf-8")

def dump_all_html():
    out_file = OUT / "estructura_y_contenido_html.txt"
    header = "📁 ESTRUCTURA DE ARCHIVOS HTML Y SU CONTENIDO\n\n"
    out_file.write_text(header, encoding="utf-8")
    html_glob = list(ROOT.rglob("fertilizantes/templates/**/*.html"))
    for html in sorted(html_glob):
        rel = html.relative_to(ROOT)
        try:
            content = html.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            content = "(No se pudo leer con UTF-8)"
        block = (
            f"\n\n🔹 Ruta: {rel}\n"
            f"📄 Archivo: {html.name}\n"
            + "="*80 +
            f"\n{content}\n\n" + "="*80 + "\n"
        )
        with out_file.open("a", encoding="utf-8") as f:
            f.write(block)

def map_endpoints():
    """Extrae rutas de urls.py y mapea a views."""
    urls_py = ROOT / "dashboard" / "urls.py"
    fert_urls_py = ROOT / "fertilizantes" / "urls.py"
    views_py = ROOT / "fertilizantes" / "views.py"

    data = ["# Mapa de endpoints (urls ↔ views)\n"]
    for upath in [urls_py, fert_urls_py]:
        if upath.exists():
            txt = upath.read_text(encoding="utf-8", errors="ignore")
            data.append(f"\n## {upath.relative_to(ROOT)}\n")
            # patrones básicos: path('ruta/', views.func, name='x')
            for m in re.finditer(r"path\(\s*['\"]([^'\"]+)['\"]\s*,\s*([a-zA-Z0-9_\.]+)", txt):
                ruta, destino = m.groups()
                data.append(f"- `{ruta}`  →  `{destino}`")
    # Listado de vistas definidas
    if views_py.exists():
        txt = views_py.read_text(encoding="utf-8", errors="ignore")
        data.append(f"\n## {views_py.relative_to(ROOT)} (def vistas)\n")
        for m in re.finditer(r"^def\s+([a-zA-Z0-9_]+)\(", txt, flags=re.MULTILINE):
            data.append(f"- `def {m.group(1)}(...)`")

    (OUT / "endpoints_map.md").write_text("\n".join(data) + "\n", encoding="utf-8")

def copy_known_sql_and_queries():
    # Si tienes el archivo central de vistas, còpialo
    for fname in ["vistas_fertilizantes.sql", "acumulado_queries.txt"]:
        src1 = ROOT / fname
        src2 = Path("/mnt/data") / fname  # por si lo mantienes fuera
        if src1.exists():
            shutil.copy2(src1, OUT / fname)
        elif src2.exists():
            shutil.copy2(src2, OUT / fname)

def dump_scripts_folder():
    out = OUT / "acumulado_scripts.txt"
    with out.open("w", encoding="utf-8") as f:
        f.write("### SCRIPTS PYTHON CONSOLIDADOS\n\n")
    if not SCRIPTS_DIR.exists():
        return
    for py in sorted(SCRIPTS_DIR.glob("*.py")):
        if py.name == "build_context_pack.py":
            continue
        content = py.read_text(encoding="utf-8", errors="ignore")
        block = f"\n\n---\n# {py.name}\n\n{content}\n"
        with out.open("a", encoding="utf-8") as f:
            f.write(block)

def export_db_schema():
    query = text("""
        SELECT
          table_name,
          column_name,
          data_type,
          is_nullable,
          COALESCE(character_maximum_length::text, '') AS char_len,
          ordinal_position
        FROM information_schema.columns
        WHERE table_schema='public'
        ORDER BY table_name, ordinal_position;
    """)
    df = pd.read_sql(query, engine)
    df.to_csv(OUT / "estructura_actual_bd.csv", index=False, encoding="utf-8")

def main():
    print(f"Generando Context Pack en: {OUT}")
    write_repo_tree()
    dump_all_html()
    map_endpoints()
    copy_known_sql_and_queries()
    dump_scripts_folder()
    export_db_schema()
    print("Listo ✅")

if __name__ == "__main__":
    main()