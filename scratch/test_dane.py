import requests
import re
import urllib3
urllib3.disable_warnings()

r = requests.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/agropecuario/sistema-de-informacion-de-precios-sipsa/componente-precios-mayoristas', verify=False)
links = re.findall(r'href=[\'"]?([^\'" >]+\.xlsx?)', r.text)
for link in list(set(links)):
    if 'mensual' in link.lower() or 'historico' in link.lower():
        print(link)
