import json

geojson_path = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/MAPAS/mexico.json"

with open(geojson_path, "r", encoding="utf-8") as f:
    data = json.load(f)

# Mostrar las primeras claves
print("Claves principales:", data.keys())

# Ver cuántos elementos hay
print("Número de features:", len(data.get("features", [])))

# Mostrar las claves de la primera entidad
if "features" in data and len(data["features"]) > 0:
    print("Ejemplo de propiedades del primer estado:")
    print(data["features"][0]["properties"])
