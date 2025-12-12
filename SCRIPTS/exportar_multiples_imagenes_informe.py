import xlwings as xw
from PIL import ImageGrab
import os
import time

# Rutas
ruta_excel = "/Users/Arturo/AGRICULTURA/INFORMES/INFORME_2025.xlsm"
carpeta_imgs = "/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales"

# Lista de hojas y nombres de imagen que queremos generar (solo las primeras 3 para prueba)
hojas_a_exportar = [
    ("Durango Avances", "grafico_3.png"),
    ("Michoacán Avances", "grafico_4.png"),
    ("Morelos Avances", "grafico_5.png"),
]

# Crear carpeta si no existe
os.makedirs(carpeta_imgs, exist_ok=True)

# Iniciar Excel
app = xw.App(visible=False)
wb = xw.Book(ruta_excel)

def exportar_imagen(nombre_hoja, nombre_archivo):
    try:
        sht = wb.sheets[nombre_hoja]

        # Limpiar imágenes anteriores en la hoja
        for shape in sht.api.Shapes:
            if shape.Type == 13:  # msoPicture = 13
                shape.Delete()

        # Copiar el rango como imagen
        sht.range("B5:Q18").api.CopyPicture(Appearance=1, Format=-4147)
        time.sleep(1)
        sht.api.Paste()
        time.sleep(1)
        sht.api.Pictures(sht.api.Pictures().Count).Copy()

        imagen = ImageGrab.grabclipboard()
        if imagen:
            ruta_img = os.path.join(carpeta_imgs, nombre_archivo)
            imagen.save(ruta_img, "PNG")
            print(f"✅ Imagen de '{nombre_hoja}' guardada como: {ruta_img}")
        else:
            print(f"❌ No se encontró imagen en portapapeles para '{nombre_hoja}'.")

    except Exception as e:
        print(f"❌ Error en hoja '{nombre_hoja}': {e}")

# Ejecutar el proceso para las hojas seleccionadas
for hoja, archivo in hojas_a_exportar:
    exportar_imagen(hoja, archivo)

wb.close()
app.quit()
