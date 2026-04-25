import requests
import pandas as pd
import urllib3
import re
urllib3.disable_warnings()

r = requests.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/agropecuario/sistema-de-informacion-de-precios-sipsa/componente-precios-mayoristas', verify=False)
links = re.findall(r'href=[\'"]?([^\'" >]+\.xlsx?)', r.text)
link = [l for l in links if 'anex-SIPSADiario' in l][0]
url = "https://www.dane.gov.co" + link

df = pd.read_excel(url, skiprows=7) # Usually these files have headers around line 7 or so
print(df.head())
print(df.columns)
