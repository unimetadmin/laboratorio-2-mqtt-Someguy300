import sys
import paho.mqtt.client
import threading
import psycopg2
from psycopg2 import Error
import datetime
import json

def on_message(client, userdata, message):
	#Traduccion de la data porque se recibe codificado
	aux = json.loads(message.payload.decode('utf-8'))
	try:
		#Conexion a la bdd para enviar
		connection = psycopg2.connect(user="yfcxxvyb",
									password="aSIUYi0BfGs7zvYwCrfOPpJNM_vsovOs",
									host="queenie.db.elephantsql.com",
									database="yfcxxvyb")

		cursor = connection.cursor()
		# Query
		insert_query = """ INSERT INTO tanque (nivel, report_time) VALUES (%s, %s)"""
		#Ajuste de tiempo para envio a bdd
		hora = datetime.datetime.now()
		hora +=  datetime.timedelta(minutes=10)
		item_tuple = (aux['nivel_Tanque'], hora)
		#Envio a bdd
		cursor.execute(insert_query, item_tuple)
		connection.commit()
		print("1 item inserted successfully")

	except (Exception, psycopg2.Error) as error:
		print("Error while connecting to PostgreSQL", error)
	finally:
		if connection:
			cursor.close()
			connection.close()
	
def on_connect(client,userdata,flags,rc):
	print('connected bano')
	client.subscribe(topic='casa/bano/#', qos=2)

def susBaño():
	client = paho.mqtt.client.Client(client_id='casa', clean_session=False)
	client.connect(host='localhost', port=1883)
	client.on_connect = on_connect
	client.on_message = on_message
	client.loop_forever()
	
#Cree las tablas aqui de una vez por si necesitaba borrarlas en algun momento
def crear_tablas():
	try:
		connection = psycopg2.connect(user="yfcxxvyb",
									password="aSIUYi0BfGs7zvYwCrfOPpJNM_vsovOs",
									host="queenie.db.elephantsql.com",
									database="yfcxxvyb")
		cursor = connection.cursor()
		create_table_query = '''CREATE TABLE IF NOT EXISTS tanque ( 
		report_id serial NOT NULL PRIMARY KEY, 
		nivel INTEGER NOT NULL,
		report_time timestamp NOT NULL
		)'''
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
	susBaño()
	


	

if __name__ == '__main__':
	main()

sys.exit(0)