import os
import fitz
import pytesseract
from PIL import Image, ImageEnhance, ImageFilter
import io
import numpy as np
import pandas as pd

RUTA_EXPEDIENTES = "/Users/Arturo/AGRICULTURA/EXPEDIENTES"
GRUPOS = ["PERSONAL SEGALMEX", "PROPUESTAS DE ALTAS"]

# Definición de claves por tipo de documento
DOCUMENTOS = {
    "CURP": ["curp"],
    "INE": ["ine", "identificacion", "credencial"],
    "RFC": ["rfc", "situacion", "fiscal"],
    "CV": ["cv", "curriculum"],
    "COMPROBANTE DE DOMICILIO": ["domicilio", "recibo"],
    "CONSTANCIA SITUACIÓN FISCAL": ["situación fiscal", "régimen fiscal", "sat"],
    "ACTA DE NACIMIENTO": ["nacimiento", "registro civil"],
    "ESTADO DE CUENTA": ["estado de cuenta", "clabe", "banco", "banamex", "bbva", "santander"],
    "CONSTANCIA DE NO INHABILITACIÓN": ["inhabilitacion", "función pública"],
    "COMPROBANTE DE ESTUDIOS": ["estudios", "titulo", "cedula"],
    "HOJA ÚNICA DE SERVICIO": ["hoja única", "servicio"],
    "CARTILLA SMN": ["cartilla", "smn"]
}

# OCR mejorado
def aplicar_ocr(pixmap):
    img_data = pixmap.tobytes("png")
    img = Image.open(io.BytesIO(img_data)).convert("L")
    img = img.filter(ImageFilter.MedianFilter())
    img = ImageEnhance.Contrast(img).enhance(2.0)
    img_np = np.array(img)
    texto = pytesseract.image_to_string(img_np, lang='spa', config='--psm 6')
    return texto.lower()

# Extracción de texto del PDF
def extraer_texto(path):
    texto_total = ""
    try:
        with fitz.open(path) as doc:
            for page in doc:
                texto = page.get_text().lower().strip()
                if len(texto) < 30:
                    pix = page.get_pixmap(dpi=400)
                    texto = aplicar_ocr(pix)
                texto_total += texto + "\n"
    except:
        pass
    return texto_total

# Clasificación por contenido
def clasificar_por_contenido(texto):
    resultados = set()
    for doc, claves in DOCUMENTOS.items():
        if any(clave in texto for clave in claves):
            resultados.add(doc)
    return resultados

# Clasificación por nombre de archivo
def clasificar_por_nombre(nombre_archivo):
    nombre = nombre_archivo.lower()
    resultados = set()
    for doc, claves in DOCUMENTOS.items():
        if any(clave in nombre for clave in claves):
            resultados.add(doc)
    return resultados

# Recorrido y análisis
checklist = []

for grupo in GRUPOS:
    ruta_grupo = os.path.join(RUTA_EXPEDIENTES, grupo)
    if not os.path.isdir(ruta_grupo):
        continue

    for persona in os.listdir(ruta_grupo):
        ruta_persona = os.path.join(ruta_grupo, persona)
        if not os.path.isdir(ruta_persona):
            continue

        documentos_detectados = {}
        observaciones = []

        archivos = [f for f in os.listdir(ruta_persona) if f.lower().endswith(".pdf")]

        for archivo in archivos:
            ruta_pdf = os.path.join(ruta_persona, archivo)
            texto = extraer_texto(ruta_pdf)

            detectados_contenido = clasificar_por_contenido(texto)
            detectados_nombre = clasificar_por_nombre(archivo)

            for doc in detectados_contenido:
                documentos_detectados[doc] = "✔️ OCR"
            for doc in detectados_nombre:
                if doc not in documentos_detectados:
                    documentos_detectados[doc] = "✔️ nombre"
                elif documentos_detectados[doc] == "✔️ nombre" and doc in detectados_contenido:
                    documentos_detectados[doc] = "✔️ OCR"

        fila = {
            "Grupo": grupo,
            "Persona": persona,
        }

        for doc in DOCUMENTOS.keys():
            fila[doc] = documentos_detectados.get(doc, "❌")

        checklist.append(fila)

# Exportar a Excel
df = pd.DataFrame(checklist)
salida = os.path.join(RUTA_EXPEDIENTES, "CHECKLIST_RESULTADO_FINAL.xlsx")
df.to_excel(salida, index=False)
print(f"✅ Checklist generado correctamente:\n{salida}")
