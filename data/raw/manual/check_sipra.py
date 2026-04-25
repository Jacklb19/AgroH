import pathlib, json, sys
sys.stdout.reconfigure(encoding='utf-8')

sipra_dir = pathlib.Path(r'c:\Users\Steven\Desktop\semestre 6\analisis de datos\hackaton1\AgroH\data\raw\manual\sipra')

for f in sipra_dir.glob('*.geojson'):
    content = f.read_text(encoding='utf-8')
    try:
        data = json.loads(content)
        features = data.get('features', [])
        tipo = data.get('type', 'N/A')
        print(f"{f.name}: {len(features)} features, tipo={tipo}")
        if features:
            props = list(features[0].get('properties', {}).keys())[:8]
            print(f"  Propiedades: {props}")
    except Exception as e:
        print(f"{f.name}: ERROR - {str(e)} | contenido: {content[:200]}")
