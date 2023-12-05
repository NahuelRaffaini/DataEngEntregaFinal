import requests

def leer_api_key():
    try:
        with open('api_key.txt', 'r') as file:
            api_key = file.read().strip()
            return api_key
    except FileNotFoundError:
        print("El archivo api_key.txt no se encontró.")
        return None

url = 'https://api.coinranking.com/v2/coins'
api_key = leer_api_key()

def obtener_datos():
    headers = {
        'x-access-token': api_key
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        coins = response.json()['data']['coins']
        
        # Filtrado para extraer los datos que me son utiles
        filtered_coins = []
        for coin in coins:
            coin_data = {
                'symbol': coin['symbol'],
                'name': coin['name'],
                'marketCap': coin['marketCap'],
                'price': coin['price'],
                'change': coin['change'],
                '24hVolume': coin['24hVolume']
            }
            filtered_coins.append(coin_data)
        
        return filtered_coins 
    else:
        print("Error al obtener datos de la API")
        return None

datos_seleccionados = obtener_datos()

if datos_seleccionados:
    print("Primer item extraído de la API:")
    print(datos_seleccionados[0])
    print(datos_seleccionados[1])
    # Imprimimos en consola los dos primeros registros extraidos