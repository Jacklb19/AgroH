import requests, urllib3
urllib3.disable_warnings()
folders = ['adecuacion_tierras_rurales', 'aptitud_uso_suelo', 'formalizacion_propiedad', 'geoprocesos', 'mercado_tierras_rurales', 'MonitoreoCultivos', 'ordenamiento_productivo', 'ordenamiento_social_propiedad', 'predios', 'referencia_geografica', 'restricciones_condicionantes', 'serviciosInteroperabilidad', 'SOE', 'tipo_utilizacion_tierra', 'uso_suelo_rural', 'Utilities']
found = False
for f in folders:
    r = requests.get(f'https://geoservicios.upra.gov.co/arcgis/rest/services/{f}?f=json', verify=False)
    if r.status_code == 200:
        services = r.json().get('services', [])
        for s in services:
            if 'cna' in s['name'].lower() or 'censo' in s['name'].lower():
                print(s['name'])
                found = True
if not found: print("No CNA found in UPRA")
