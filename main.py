import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Datos de ejemplo, reemplazar con tu DataFrame
data = {
    'NumeroPersonal': ['200144595',' 200144597', '200144599', '200144635','200144642', '200144667', '200144672', '200144699', '200144801', '200144807'],  # Completar con tus datos
    'Direccion': [
        'AV. EL EJERCITO 130 URB. EL MOLINO',
        'CALLE JOSE GALCEZ 136 URB. VISTA ALEGRE',
        'AMPLIACION SANCHEZ CERRO B1-16 TALARA ALTA',
        'JR SAN MARTIN 325 SANTA ROSA',
        'ASOC VIA DE ATE MZ. C LOTE 2',
        'MALECON RIMAC 221 MZ B LT 26',
        'AV. SAN JUAN 751 URB SAN JUAN',
        'MZ F LT 24 URB SAN AGUSTÁN',
        'AV. FERROCARRIL SUR 2428',
        'JR CORONEL FRANCISCO BOLOGNESI SEGUNDA CUADRA'
    ]
}

df = pd.DataFrame(data)

subscription_key = 'H8_omfGD_JMD24A9Iifq-oGXrJ5iyTcYEsSjfyFqdF8'
url = "https://atlas.microsoft.com/search/address/json"

def geocodificar(direccion):
    params = {
        'api-version': '1.0',
        'subscription-key': subscription_key,
        'query': direccion
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        results = response.json()
        if results['results']:
            position = results['results'][0]['position']
            return position['lat'], position['lon']
    return None, None

def process_addresses(df):
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(geocodificar, df['Direccion']))
    return results

# Aplicar geocodificación
df['Latitud'], df['Longitud'] = zip(*process_addresses(df))

print(df)
