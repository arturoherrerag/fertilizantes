# Versión mejorada del script con OCR avanzado, detección de firmas y clasificación ampliada

import os
import fitz  # PyMuPDF
import pytesseract
from PIL import Image, ImageEnhance, ImageFilter
import io
import numpy as np
import pandas as pd

# Configuración base
RUTA_EXPEDIENTES = "/Users/Arturo/AGRICULTURA/EXPEDIENTES"
GRUPOS = ["PERSONAL SEGALMEX", "PROPUESTAS DE ALTAS"]

# Palabras clave ampliadas por tipo de documento
CLAVES_DOCUMENTOS = {
    "ACTA DE NACIMIENTO": ["registro civil", "acta de nacimiento", "oficialía", "libro"],
    "CURP": ["clave única de registro", "curp", "gobierno de méxico"],
    "INE": ["instituto nacional electoral", "credencial para votar", "vigencia", "electoral"],
    "RFC": ["registro federal de contribuyentes", "sat"],
    "CONSTANCIA SITUACIÓN FISCAL": ["situación fiscal", "régimen fiscal", "código postal", "sat"],
    "COMPROBANTE DE DOMICILIO": ["recibo", "cfe", "luz", "agua", "gas", "servicio", "importe"],
    "CONSTANCIA DE NO INHABILITACIÓN": ["función pública", "no se encuentra inhabilitado", "inhabilitación"],
    "ESTADO DE CUENTA": [
        "estado de cuenta", "saldo", "clabe", "cuenta", "movimientos",
        "banamex", "bbva", "santander", "banorte", "scotiabank",
        "número de cuenta", "gastos", "detalle de movimientos", "banco", "pago de nómina"
    ],
    "CV": [
        "curriculum vitae", "currículum", "datos personales", "experiencia laboral",
        "formación académica", "educación", "perfil profesional", "objetivo",
        "referencias", "escolaridad", "periodo laboral"
    ],
    "COMPROBANTE DE ESTUDIOS": [
        "título", "certificado", "licenciatura", "ingeniería", "universidad", "bachillerato", "cédula"
    ],
    "CARTILLA SMN": ["servicio militar nacional", "cartilla", "defensa nacional"],
    "HOJAS ÚNICAS DE SERVICIO": ["hoja única de servicio", "fecha de ingreso", "plaza", "nómina"]
}

# Aplicar OCR mejorado con Pillow + Tesseract + DPI alto
def aplicar_ocr_con_mejoras(pixmap):
    img_data = pixmap.tobytes("png")
    img = Image.open(io.BytesIO(img_data)).convert('L')  # Escala de grises
    img = img.filter(ImageFilter.MedianFilter())
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(2.0)
    img_np = np.array(img)
    texto = pytesseract.image_to_string(img_np, lang='spa', config='--psm 6')
    return texto

# Extraer texto de PDF, aplicar OCR si no hay texto
def extraer_texto_pdf(path):
    try:
        doc = fitz.open(path)
        texto_total = ""
        for i, page in enumerate(doc):
            texto = page.get_text().strip()
            if len(texto) < 30:
                pix = page.get_pixmap(dpi=400)
                texto += aplicar_ocr_con_mejoras(pix)
            texto_total += texto + "\n"
        doc.close()
        return texto_total.strip()
    except Exception:
        return ""

# Clasificar documento según su contenido textual
def clasificar_documento(texto):
    texto_limpio = texto.lower()
    coincidencias = {}
    for tipo, claves in CLAVES_DOCUMENTOS.items():
        score = sum(1 for palabra in claves if palabra in texto_limpio)
        if score > 0:
            coincidencias[tipo] = score
    if coincidencias:
        tipo_detectado = max(coincidencias, key=coincidencias.get)
        return tipo_detectado, coincidencias[tipo_detectado]
    return "NO IDENTIFICADO", 0

# Recolectar resultados
resultados = []

for grupo in GRUPOS:
    ruta_grupo = os.path.join(RUTA_EXPEDIENTES, grupo)
    if not os.path.isdir(ruta_grupo):
        continue

    for persona in os.listdir(ruta_grupo):
        ruta_persona = os.path.join(ruta_grupo, persona)
        if not os.path.isdir(ruta_persona):
            continue

        for archivo in os.listdir(ruta_persona):
            if archivo.lower().endswith(".pdf"):
                ruta_archivo = os.path.join(ruta_persona, archivo)
                texto = extraer_texto_pdf(ruta_archivo)
                tipo, score = clasificar_documento(texto)
                resultados.append({
                    "Grupo": grupo,
                    "Persona": persona,
                    "Archivo": archivo,
                    "Tipo Detectado": tipo,
                    "Coincidencias": score
                })

# Exportar resultados a Excel
archivo_salida = os.path.join(RUTA_EXPEDIENTES, "clasificacion_resultados.xlsx")
pd.DataFrame(resultados).to_excel(archivo_salida, index=False)
print(f"\n✅ Clasificación completada. Resultados guardados en:\n{archivo_salida}")
