import pandas as pd
import requests
import re
import urllib3
urllib3.disable_warnings()
from datetime import datetime

r = requests.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/agropecuario/sistema-de-informacion-de-precios-sipsa/componente-precios-mayoristas', verify=False)
links = re.findall(r'href=[\'"]?([^\'" >]+\.xlsx?)', r.text)
daily_links = [l for l in links if 'anex-SIPSADiario' in l]

url = "https://www.dane.gov.co" + daily_links[0]
df = pd.read_excel(url, header=None)

fecha_str = str(df.iloc[1, 0])  # "Viernes 24 de abril de 2026"

# Parse cities from row 2
ciudades = {}
for col in range(1, len(df.columns), 2):
    ciudad = str(df.iloc[2, col]).strip()
    if ciudad != 'nan':
        ciudades[col] = ciudad

records = []
for idx in range(4, len(df)):
    producto = str(df.iloc[idx, 0]).strip()
    # Skip category headers or empty
    if pd.isna(df.iloc[idx, 1]) or producto == 'nan' or 'Fuente:' in producto:
        continue
    
    for col, ciudad in ciudades.items():
        precio = df.iloc[idx, col]
        if pd.notna(precio) and str(precio).strip() != 'n.d.':
            records.append({
                'fecha_registro': fecha_str,
                'producto': producto,
                'central': ciudad,
                'ciudad': ciudad.split(',')[0],  # "Bogotá, Corabastos" -> "Bogotá"
                'precio_promedio_cop_kg': precio
            })

df_flat = pd.DataFrame(records)
print(df_flat.head())
print("Total records:", len(df_flat))
