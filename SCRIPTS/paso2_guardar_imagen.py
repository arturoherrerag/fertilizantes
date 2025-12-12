from PIL import ImageGrab
import os

ruta_img = "/Users/Arturo/AGRICULTURA/imagen_temp_avance.png"

# Capturar imagen desde el portapapeles
imagen = ImageGrab.grabclipboard()

if not imagen:
    print("❌ No se encontró imagen en el portapapeles. Asegúrate de ejecutar la macro antes.")
else:
    imagen.save(ruta_img, "PNG")
    print(f"✅ Imagen guardada en: {ruta_img}")
