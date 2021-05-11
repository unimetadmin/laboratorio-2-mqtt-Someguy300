import sys
import paho.mqtt.client
import threading
import psycopg2
from psycopg2 import Error
import datetime
import json

def on_messageContador(client, userdata, message):
	aux = json.loads(message.payload.decode('utf-8'))
	try:
		connection = psycopg2.connect(user="yfcxxvyb",
									password="aSIUYi0BfGs7zvYwCrfOPpJNM_vsovOs",
									host="queenie.db.elephantsql.com",
									database="yfcxxvyb")

		cursor = connection.cursor()
		insert_query = """ INSERT INTO contador_pers (nropersonas, report_time) VALUES (%s, %s)"""
		hora = datetime.datetime.now()
		hora +=  datetime.timedelta(minutes=1)
		item_tuple = (aux['nro_Personas'], hora)
		cursor.execute(insert_query, item_tuple)
		connection.commit()
		print("1 item inserted successfully Contador")

	except (Exception, psycopg2.Error) as error:
		print("Error while connecting to PostgreSQL", error)
	finally:
		if connection:
			cursor.close()
			connection.close()

def on_messageAlexa(client, userdata, message):
	aux = json.loads(message.payload.decode('utf-8'))
	try:
		connection = psycopg2.connect(user="yfcxxvyb",
									password="aSIUYi0BfGs7zvYwCrfOPpJNM_vsovOs",
									host="queenie.db.elephantsql.com",
									database="yfcxxvyb")

		cursor = connection.cursor()
		insert_query = """ INSERT INTO alexa_echo (temperaturaccs, report_time) VALUES (%s, %s)"""
		hora = datetime.datetime.now()
		hora +=  datetime.timedelta(minutes=10)
		item_tuple = (aux['temperatura_Caracas'], hora)
		cursor.execute(insert_query, item_tuple)
		connection.commit()
		print("1 item inserted successfully Alexa")

	except (Exception, psycopg2.Error) as error:
		print("Error while connecting to PostgreSQL", error)
	finally:
		if connection:
			cursor.close()
			connection.close()




def susContador():
	client = paho.mqtt.client.Client(client_id='sala-contador', clean_session=False)
	client.connect(host='localhost', port=1883)
	print('connected sala/contador')
	client.subscribe(topic='casa/sala/contador_personas', qos=2)
	client.on_message = on_messageContador
	client.loop_forever()

def susAlexa():
	client = paho.mqtt.client.Client(client_id='sala-alexa', clean_session=False)
	client.connect(host='localhost', port=1883)
	print('connected sala/alexa')
	client.subscribe(topic='casa/sala/alexa_echo', qos=2)
	client.on_message = on_messageAlexa
	client.loop_forever()
	

def crear_tablas():
	try:
		connection = psycopg2.connect(user="yfcxxvyb",
									password="aSIUYi0BfGs7zvYwCrfOPpJNM_vsovOs",
									host="queenie.db.elephantsql.com",
									database="yfcxxvyb")
		cursor = connection.cursor()
		

		create_table_query = '''CREATE TABLE IF NOT EXISTS contador_pers ( 
		report_id serial NOT NULL PRIMARY KEY, 
		nropersonas INTEGER NOT NULL,
		report_time timestamp NOT NULL
		) '''
		cursor.execute(create_table_query)
		connection.commit()
		

		create_table_query = '''CREATE TABLE IF NOT EXISTS alexa_echo ( 
		report_id serial NOT NULL PRIMARY KEY, 
		temperaturaccs INTEGER NOT NULL,
		report_time timestamp NOT NULL
		) '''
		cursor.execute(create_table_query)
		connection.commit()
		

	except (Exception, Error) as error:
		print("Error while connecting to PostgreSQL", error)
	finally:
		if connection:
			cursor.close()
			connection.close()
			print("PostgreSQL connection is closed")

def main():
	crear_tablas()
	x = threading.Thread(target=susContador)
	x.start()
	y = threading.Thread(target=susAlexa)
	y.start()
	


	

if __name__ == '__main__':
	main()

sys.exit(0)