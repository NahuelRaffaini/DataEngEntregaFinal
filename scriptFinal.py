import pandas as pd
import psycopg2
from psycopg2 import sql
import requests
from datetime import datetime
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logging.basicConfig(level=logging.INFO)

def leer_api_key():
    try:
        api_key = os.environ.get('API_KEY')
        if api_key is None:
            raise ValueError("La variable de entorno API_KEY no está definida.")
        return api_key
    except ValueError as e:
        logging.error(e)
        return None

def leer_psw():
    try:
        psw = os.environ.get('DB_PASSWORD')
        if psw is None:
            raise ValueError("La variable de entorno DB_PASSWORD no está definida.")
        return psw
    except ValueError as e:
        logging.error(e)
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

def get_api_data():
    api_key = leer_api_key()
    if not api_key:
        return []
    return obtener_datos(api_key)

def process_data(datos_seleccionados):
    if not datos_seleccionados:
        return pd.DataFrame()
    df = pd.DataFrame(datos_seleccionados)
    return df

import json

def cargar_configuracion():
    with open('config_alertas.json', 'r') as file:
        return json.load(file)

def check_btc_alert(df):
    config = cargar_configuracion()
    btc_config = config['alertas']['BTC']
    limite_subida = btc_config['limite_subida']
    limite_bajada = btc_config['limite_bajada']
    mensaje_template = btc_config['mensaje']

    btc_row = df[df['symbol'] == 'BTC']
    if not btc_row.empty:
        cambio = btc_row.iloc[0]['change']
        precio_actual = btc_row.iloc[0]['price']
        if cambio > limite_subida or cambio < limite_bajada:
            accion = "subido" if cambio > 0 else "bajado"
            mensaje = mensaje_template.format(accion=accion, cambio=abs(cambio), precio_actual=precio_actual, url_detalle="https://www.ejemplo.com/detalle/BTC")
            send_alert_email(mensaje)


def send_alert_email(mensaje):
    host = os.environ.get('EMAIL_HOST')
    port = os.environ.get('EMAIL_PORT')
    user = os.environ.get('EMAIL_HOST_USER')
    password = os.environ.get('EMAIL_HOST_PASSWORD')
    receiver = os.environ.get('EMAIL_RECEIVER')

    msg = MIMEMultipart()
    msg['From'] = user
    msg['To'] = receiver
    msg['Subject'] = 'Alerta de cambio de precio de Bitcoin'
    msg.attach(MIMEText(mensaje, 'plain'))

    try:
        if port == "587":
            server = smtplib.SMTP(host, port)
            server.starttls() 
        else:
            server = smtplib.SMTP_SSL(host, port)
        server.login(user, password)
        server.sendmail(user, receiver, msg.as_string())
        print('Alerta de correo electrónico enviada.')
    except Exception as e:
        print(f"Error al enviar el correo electrónico: {e}")
    finally:
        server.quit()


def insert_into_db(df):
    if df.empty:
        logging.info("No hay datos para insertar en la base de datos.")
        return
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

