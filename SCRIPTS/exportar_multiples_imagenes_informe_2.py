import xlwings as xw
from PIL import ImageGrab
import os
import time

ruta_excel = "/Users/Arturo/AGRICULTURA/INFORMES/INFORME_2025.xlsm"
carpeta_imgs = "/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales"

# Lista de hojas y nombres de imagen que queremos generar
hojas_a_exportar = [
    ("Durango Avances", "grafico_3.png"),
    ("Michoacán Avances", "grafico_4.png"),
    ("Morelos Avances", "grafico_5.png"),
]

os.makedirs(carpeta_imgs, exist_ok=True)

app = xw.App(visible=False)
wb = xw.Book(ruta_excel)

def exportar_imagen_via_macro(nombre_hoja, nombre_archivo):
    try:
        macro = wb.macro("ExportarImagenDeHoja")
        macro(nombre_hoja)

        time.sleep(1.5)
        imagen = ImageGrab.grabclipboard()
        if imagen:
            ruta_img = os.path.join(carpeta_imgs, nombre_archivo)
            imagen.save(ruta_img, "PNG")
            print(f"✅ Imagen de '{nombre_hoja}' guardada como: {ruta_img}")
        else:
            print(f"❌ No se encontró imagen en portapapeles para '{nombre_hoja}'.")

    except Exception as e:
        print(f"❌ Error con la hoja '{nombre_hoja}': {e}")

for hoja, archivo in hojas_a_exportar:
    exportar_imagen_via_macro(hoja, archivo)

wb.close()
app.quit()
