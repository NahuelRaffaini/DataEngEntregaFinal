#Script para segunda pre-entrega

from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2 import sql
import requests

def leer_api_key():
    try:
        with open('api_key.txt', 'r') as file:
            api_key = file.read().strip()  # Lee la clave de API y elimina espacios en blanco adicionales
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
        
        # Filtrado para extraer datos específicos
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
    fecha_actual = datetime.now().date()
    for dato in datos_seleccionados:
        dato['fecha'] = fecha_actual
    df = pd.DataFrame(datos_seleccionados)
else:
    df = pd.DataFrame() 

def leer_psw():
    try:
        with open('psw.txt', 'r') as file:
            psw = file.read().strip()
            return psw
    except FileNotFoundError:
        print("El archivo psw.txt no se encontró.")
        return None

try:
    conn = psycopg2.connect(
        dbname='data-engineer-database',
        user='nahuelraffaini_coderhouse',
        password= leer_psw(),
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        port='5439'
    )
    print("Conexión exitosa a Redshift")
except psycopg2.Error as e:
    print("Error al conectarse a Redshift:", e)

cur = conn.cursor()

truncate_query = sql.SQL('''
    TRUNCATE TABLE nahuelraffaini_coderhouse.coinscryptosnr
''')
cur.execute(truncate_query)
conn.commit()

if not df.empty:
    df.drop_duplicates(subset='symbol', keep='first', inplace=True)

if not df.empty:
    insert_query = sql.SQL('''
    INSERT INTO nahuelraffaini_coderhouse.coinscryptosnr (symbol, name, marketCap, price, change, "24hVolume", fecha) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ''')
    data_values = df.values.tolist()
    cur.executemany(insert_query, data_values)
    conn.commit()
    print("Datos insertados desde el DataFrame en Redshift exitosamente")

cur.close()
conn.close()