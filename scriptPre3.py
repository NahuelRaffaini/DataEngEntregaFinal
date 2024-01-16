import pandas as pd
import psycopg2
from psycopg2 import sql
import requests
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def leer_api_key():
    try:
        with open('api_key.txt', 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        logging.error("El archivo api_key.txt no se encontró.")
        return None

def leer_psw():
    try:
        with open('psw.txt', 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        logging.error("El archivo psw.txt no se encontró.")
        return None

def obtener_datos(api_key):
    url = 'https://api.coinranking.com/v2/coins'
    headers = {'x-access-token': api_key}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        coins = response.json()['data']['coins']
        return [{ 
            'symbol': coin['symbol'],
            'name': coin['name'],
            'marketCap': coin['marketCap'],
            'price': round(float(coin['price']), 3),
            'change': coin['change'],
            '24hVolume': coin['24hVolume'],
            'fecha': datetime.now().date()
        } for coin in coins]
    except requests.RequestException as e:
        logging.error(f"Error al obtener datos de la API: {e}")
        return []

def main():
    api_key = leer_api_key()
    if not api_key:
        return

    datos_seleccionados = obtener_datos(api_key)
    if not datos_seleccionados:
        return

    df = pd.DataFrame(datos_seleccionados)

    psw = leer_psw()
    if not psw:
        return

    try:
        conn = psycopg2.connect(
            dbname='data-engineer-database',
            user='nahuelraffaini_coderhouse',
            password=psw,
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            port='5439'
        )
        cur = conn.cursor()

        transfer_query = sql.SQL('''
            INSERT INTO nahuelraffaini_coderhouse.historycryptosnr
            SELECT * FROM nahuelraffaini_coderhouse.coinscryptosnr
            WHERE NOT EXISTS (
                SELECT 1 FROM nahuelraffaini_coderhouse.historycryptosnr h
                WHERE h.symbol = coinscryptosnr.symbol AND h.fecha = coinscryptosnr.fecha
            );
        ''')
        cur.execute(transfer_query)
        conn.commit()

        delete_query = sql.SQL('''
            DELETE FROM nahuelraffaini_coderhouse.coinscryptosnr
        ''')
        cur.execute(delete_query)
        conn.commit()

        if not df.empty:
            df.drop_duplicates(subset='symbol', keep='first', inplace=True)
            insert_query = sql.SQL('''
            INSERT INTO nahuelraffaini_coderhouse.coinscryptosnr (symbol, name, marketCap, price, change, "24hVolume", fecha) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''')
            data_values = df.values.tolist()
            cur.executemany(insert_query, data_values)
            conn.commit()
            logging.info("Datos insertados desde el DataFrame en Redshift exitosamente")

    except psycopg2.Error as e:
        logging.error(f"Error al conectarse a Redshift: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()
