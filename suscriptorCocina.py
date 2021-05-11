import sys
import paho.mqtt.client
import threading
import psycopg2
from psycopg2 import Error
import datetime
import json

def on_messageNevera(client, userdata, message):
	aux = json.loads(message.payload.decode('utf-8'))
	try:
		connection = psycopg2.connect(user="yfcxxvyb",
									password="aSIUYi0BfGs7zvYwCrfOPpJNM_vsovOs",
									host="queenie.db.elephantsql.com",
									database="yfcxxvyb")

		cursor = connection.cursor()
		# Executing a SQL query to insert datetime into table
		insert_query = """ INSERT INTO temp_nevera (temperatura, report_time) VALUES (%s, %s)"""
		hora = datetime.datetime.now()
		hora +=  datetime.timedelta(minutes=5)
		item_tuple = (aux['tempNevera'], hora)
		cursor.execute(insert_query, item_tuple)
		connection.commit()
		print("1 item inserted successfully")

	except (Exception, psycopg2.Error) as error:
		print("Error while connecting to PostgreSQL", error)
	finally:
		if connection:
			cursor.close()
			connection.close()

def on_messageOlla(client, userdata, message):
	aux = json.loads(message.payload.decode('utf-8'))
	try:
		connection = psycopg2.connect(user="yfcxxvyb",
									password="aSIUYi0BfGs7zvYwCrfOPpJNM_vsovOs",
									host="queenie.db.elephantsql.com",
									database="yfcxxvyb")

		cursor = connection.cursor()
		# Executing a SQL query to insert datetime into table
		insert_query = """ INSERT INTO temp_olla (temperatura, report_time) VALUES (%s, %s)"""
		hora = datetime.datetime.now()
		item_tuple = (aux['temperatura_Olla'], hora)
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
	client.subscribe(topic='casa/cocina/#', qos=2)


def susCocinaNevera():
	client = paho.mqtt.client.Client(client_id='cocina_nevera', clean_session=False)
	client.connect(host='localhost', port=1883)
	print('connected cocina/nevera')
	client.subscribe(topic='casa/cocina/temperatura_nevera', qos=2)
	client.on_message = on_messageNevera
	client.loop_forever()

def susCocinaOlla():
	client = paho.mqtt.client.Client(client_id='cocina', clean_session=False)
	client.connect(host='localhost', port=1883)
	print('connected cocina/olla')
	client.subscribe(topic='casa/cocina/temperatura_olla', qos=2)
	client.on_message = on_messageOlla
	client.loop_forever()
	

def crear_tablas():
	try:
		connection = psycopg2.connect(user="yfcxxvyb",
									password="aSIUYi0BfGs7zvYwCrfOPpJNM_vsovOs",
									host="queenie.db.elephantsql.com",
									database="yfcxxvyb")
		cursor = connection.cursor()
		

		create_table_query = '''CREATE TABLE IF NOT EXISTS temp_nevera ( 
		report_id serial NOT NULL PRIMARY KEY, 
		temperatura INTEGER NOT NULL,
		report_time timestamp NOT NULL
		) '''
		cursor.execute(create_table_query)
		connection.commit()
		

		create_table_query = '''CREATE TABLE IF NOT EXISTS temp_olla ( 
		report_id serial NOT NULL PRIMARY KEY, 
		temperatura INTEGER NOT NULL,
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
	x = threading.Thread(target=susCocinaNevera)
	x.start()
	y = threading.Thread(target=susCocinaOlla)
	y.start()
	


	

if __name__ == '__main__':
	main()

sys.exit(0)