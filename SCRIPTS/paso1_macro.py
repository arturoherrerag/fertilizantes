import xlwings as xw

ruta_excel = "/Users/Arturo/AGRICULTURA/INFORMES/INFORME_2025.xlsm"

app = xw.App(visible=False)
wb = xw.Book(ruta_excel)

try:
    macro = wb.macro("ExportarImagenGuerrero")
    macro()
    print("✅ Macro 'ExportarImagenGuerrero' ejecutada con éxito desde Python.")
except Exception as e:
    print(f"❌ Error al ejecutar la macro: {e}")

wb.close()
app.quit()
