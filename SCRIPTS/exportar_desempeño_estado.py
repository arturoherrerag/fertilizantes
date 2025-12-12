import subprocess

archivo_excel = "/Users/Arturo/AGRICULTURA/SEGUIMIENTO 2025/Desempeño entregas Nacional.xlsm"
macro = "ExportarPDFsPorEstado"

apple_script = f'''
tell application "Microsoft Excel"
    activate
    open POSIX file "{archivo_excel}"
    run VBScript "{macro}"
end tell
'''

subprocess.run(["osascript", "-e", apple_script])
print("🟢 Proceso iniciado. Espera a que Excel exporte los PDFs.")
