import pandas as pd
import xlwings as xw

# Cargar CSV
df = pd.read_csv(
    "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_padrones_compilado/PARTE1.csv",
    low_memory=False,  # evita el warning de tipos mezclados
    encoding="utf-8"   # asegúrate de leer con UTF-8
)

# Mostrar primeras filas
print("Primeras filas del archivo:")
print(df.head(100))  # puedes cambiar el número de filas

# Mostrar resumen de columnas y tipos de datos
print("\nResumen de columnas:")
print(df.info())
print(df.describe())
print(df['UREA (ton)'].sum())
print(df.columns)
print(df['estado_predio_Inegi'].unique())
print(df['estado_predio_Inegi'].value_counts())
print(df[(df['estado_predio_Inegi'] == 'CHIAPAS') | (df['estado_predio_Inegi'] == 'HIDALGO')])
estados = df['estado_predio_Inegi'].unique()
print(estados)
for estado in estados:
    df_estado = df[df['estado_predio_Inegi']==estado]
    nombre_archivo = f'registros_{estado.lower()}.xlsx'
    df_estado.to_excel(nombre_archivo,index = False)
    print(f'archivo guardado: {nombre_archivo} con {len(df_estado)} registros')