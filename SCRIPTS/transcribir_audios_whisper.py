import os
from pathlib import Path
import time
from faster_whisper import WhisperModel

# Ruta del audio
AUDIO = Path("/Users/Arturo/Desktop/Grabación de pantalla 2025-11-14 a la(s) 12.01.43.mov")

# Carpeta de salida
OUT_DIR = AUDIO.parent
MODEL_SIZE = "medium"   # Equilibrio entre calidad y velocidad

# Cargar modelo en CPU con int8 (rápido en M-series)
print("Cargando modelo más rápido (faster-whisper, int8 en CPU)...")
model = WhisperModel(MODEL_SIZE, device="cpu", compute_type="int8")

# Variables para progreso
texto_total = []
segmentos = []
t0 = time.time()
contador = 0

# Transcribir con detección de voz y beam search para más precisión
for seg in model.transcribe(
    str(AUDIO),
    language="es",
    vad_filter=True,
    beam_size=5,
    condition_on_previous_text=True
)[0]:
    contador += 1
    texto_total.append(seg.text)
    segmentos.append({"start": seg.start, "end": seg.end, "text": seg.text})

    # Mostrar progreso cada 10 segmentos
    if contador % 10 == 0:
        transcurrido = time.time() - t0
        mins, secs = divmod(int(transcurrido), 60)
        print(f"🕒 Segmentos procesados: {contador} | Tiempo transcurrido: {mins:02d}:{secs:02d}")

# Guardar texto corrido
out_txt = OUT_DIR / "reunion_coordinacion_14112025.txt"
out_txt.write_text(" ".join(texto_total).strip(), encoding="utf-8")

# Guardar versión segmentada con tiempos
out_seg = OUT_DIR / "reunion_coordinacion_14112025_tiempos.txt"
with out_seg.open("w", encoding="utf-8") as f:
    for s in segmentos:
        m_ini, s_ini = divmod(int(s["start"]), 60)
        m_fin, s_fin = divmod(int(s["end"]), 60)
        f.write(f"[{m_ini:02d}:{s_ini:02d} - {m_fin:02d}:{s_fin:02d}] {s['text'].strip()}\n")

print(f"\n✅ Texto completo: {out_txt}")
print(f"✅ Segmentos con tiempos: {out_seg}")   