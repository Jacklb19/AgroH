import requests, re, urllib3
urllib3.disable_warnings()
r = requests.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/agropecuario/censo-nacional-agropecuario-2014', verify=False)
links = re.findall(r'href=[\'"]?([^\'" >]+\.(?:xlsx?|zip|csv))', r.text)
for l in list(set(links)):
    print(l)
