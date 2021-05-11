import ssl
import sys
import json
import random
import time
import paho.mqtt.client
import paho.mqtt.publish
import numpy as np
import threading
import requests

def on_connect(client, userdata, flags, rc):
    print('conectado publicador')

def cocina():
    #Crea cliente mqtt publisher
    client = paho.mqtt.client.Client("Casa", False)
    client.qos = 0
    client.connect(host='localhost')
    while(True):   
        #Variables que cambian
        tempNevera = np.random.uniform(8, 12)
        capHielo = np.random.uniform(0, 10)
        #Definicion y enviado payload
        payload = {
        "tempNevera": int(tempNevera)
        }
        client.publish('casa/cocina/temperatura_nevera', json.dumps(payload), qos=0)
        print(payload)

        payload = {
        "capacidad_Hielo": int(capHielo)
        }
        client.publish('casa/cocina/capacidad_hielo', json.dumps(payload), qos=0)
        print(payload)
        #Para que los datos no se envien tan rapido
        time.sleep(5)

def olla():
    client = paho.mqtt.client.Client("Casa", False)
    client.qos = 0
    client.connect(host='localhost')
    while(True):
        tempOlla = np.random.uniform(0, 150)
        if tempOlla > 100:
            payload = {
                "temperatura_Olla": int(tempOlla),
                "alerta": "AGUA HIRVIENDO"
            }
            client.publish('casa/cocina/temperatura_olla', json.dumps(payload), qos=0)
        else:
            payload = {
                "temperatura_Olla": int(tempOlla),
            }
            client.publish('casa/cocina/temperatura_olla', json.dumps(payload), qos=0)
        print(payload)
        time.sleep(1)

def sala():
    client = paho.mqtt.client.Client("Casa", False)
    client.qos = 0
    client.connect(host='localhost')
    response = requests.get("https://api.openweathermap.org/data/2.5/weather?q=caracas,ve&appid=3c0435647b6b1abe2ea0791915a5466d")
    #SALA
    #Contador personas
    while(True):
        contPersonas = np.random.uniform(0, 10)
        if contPersonas > 5:
            payload = {
                "nro_Personas": int(contPersonas),
                "alerta": "Mas de 5 personas presentes"
            }
            client.publish('casa/sala/contador_personas', json.dumps(payload), qos=0)
        else:
            payload = {
                "nro_Personas": int(contPersonas),

            }
            client.publish('casa/sala/contador_personas', json.dumps(payload), qos=0)
        print(payload)
        #alexa echo
        alexaEcho = response.json()['main']['temp']/10
        payload = {
            "temperatura_Caracas": alexaEcho,
        }
        client.publish('casa/sala/alexa_echo', json.dumps(payload), qos=0)
        print(payload)
        time.sleep(5)

def bano(nivelTanque):
    client = paho.mqtt.client.Client("Casa", False)
    client.qos = 0
    client.connect(host='localhost')
    i = 0
    while(True):
        if i == 3:
            nivelTanque += np.random.uniform(15, 5)
            i=0
        else:
            nivelTanque -= np.random.uniform(5, 15)
            i += 1
            if nivelTanque <= 50:
                payload = {
                    "alerta_Tanque": "Agua por debajo del 50%",
                    "nivel_Tanque": int(nivelTanque),
                }
                client.publish('casa/bano/tanque', json.dumps(payload), qos=0)
                print(payload)  
            else:
                payload = {
                    "nivel_Tanque": int(nivelTanque),
                }
                client.publish('casa/bano/tanque', json.dumps(payload), qos=0)
                print(payload)
        time.sleep(5)
            
            


def main():
    #Inicialmente pense que de verdad c/dato tenia que ser enviado
    # c/5 min o c/10 min por lo que use hilos para poder manejar
    # ese proceso mas facilmente 
    w = threading.Thread(target=cocina)
    w.start()
    x = threading.Thread(target=olla)
    x.start()
    y = threading.Thread(target=sala)
    y.start()
    z = threading.Thread(target=bano(100))
    z.start()

if __name__ == '__main__':
    main()
    sys.exit(0)
