import urllib3
urllib3.disable_warnings()
import requests
import pandas as pd

MAP_UPRA = {'Arroz': 'aptitud_arroz_secano', 'Cafe': 'Aptitud_Cafe_Jul2022'}
dfs = []
for cultivo, layer in MAP_UPRA.items():
    url = f'https://geoservicios.upra.gov.co/arcgis/rest/services/aptitud_uso_suelo/{layer}/MapServer/0/query'
    params = {'where': '1=1', 'outFields': 'cod_dane_mpio,aptitud', 'f': 'json', 'returnGeometry': 'false'}
    r = requests.get(url, params=params, verify=False)
    data = r.json()
    if 'features' in data:
        records = [f['attributes'] for f in data['features']]
        df = pd.DataFrame(records)
        df['cultivo_upra'] = cultivo
        dfs.append(df)
print(pd.concat(dfs).head())
