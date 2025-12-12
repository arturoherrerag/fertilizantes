import xlwings as xw
from PIL import ImageGrab
import os

# Rutas
ruta_excel = "/Users/Arturo/AGRICULTURA/INFORMES/INFORME_2025.xlsm"
carpeta_imgs = "/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales"
ruta_img1 = os.path.join(carpeta_imgs, "grafico_1.png")
ruta_img2 = os.path.join(carpeta_imgs, "grafico_2.png")

# Asegurarse que la carpeta exista
os.makedirs(carpeta_imgs, exist_ok=True)

# Iniciar app de Excel
app = xw.App(visible=False)
wb = xw.Book(ruta_excel)

def ejecutar_macro_y_guardar(nombre_macro, ruta_imagen):
    try:
        macro = wb.macro(nombre_macro)
        macro()
        print(f"✅ Macro '{nombre_macro}' ejecutada.")

        # Espera ligera para asegurar que la imagen llegue al portapapeles
        import time; time.sleep(1)

        imagen = ImageGrab.grabclipboard()
        if imagen:
            imagen.save(ruta_imagen, "PNG")
            print(f"✅ Imagen guardada como: {ruta_imagen}")
        else:
            print(f"❌ No se encontró imagen en el portapapeles tras ejecutar '{nombre_macro}'.")

    except Exception as e:
        print(f"❌ Error ejecutando '{nombre_macro}': {e}")

# Ejecutar macros y guardar imágenes
ejecutar_macro_y_guardar("ExportarImagenDesdeRango", ruta_img1)
ejecutar_macro_y_guardar("ExportarImagenGuerrero", ruta_img2)

# Cerrar archivo y Excel
wb.close()
app.quit()
