import requests
r = requests.get('https://www.datos.gov.co/api/views?limit=50&q=agronet+precios')
if r.status_code == 200:
    data = r.json()
    for d in data:
        print(d['id'], d['name'])
