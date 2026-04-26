import requests
import pandas as pd

url = "https://www.datos.gov.co/resource/uejq-wxrr.json"
params = {"$limit": 5}
r = requests.get(url, params=params)
data = r.json()
df = pd.DataFrame(data)
print(df.columns)
print(df[['a_o', 'municipio', 'cultivo']].head())
