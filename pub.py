import ssl
import sys
import json
import random
import time
import paho.mqtt.client
import paho.mqtt.publish
import numpy as np
from datetime import datetime, date, timedelta
import psycopg2


def on_connect(client, userdata, flags, rc):
	print('connected')

def main():
	#configuración de la comuncación con protocolo mqtt
	client = paho.mqtt.client.Client("Unimet", False)
	client.qos = 0
	client.connect(host='localhost')

	#Peso max y min de cada envio en kgs
	meanTonelada = 5
	precioOroKg=42900
	stdTonelada = 2

	repetir=2500

	#Para conectarnos con la base de datos en postgres
	conn = psycopg2.connect(host = 'localhost', user= 'postgres', password ='toby3030', dbname= 'cerveceria')
	#conn = psycopg2.connect(host = 'ec2-107-20-167-11.compute-1.amazonaws.com', user= 'inytxpqzmlwhkp', password ='167ff1fd25c60913b54178a1c3427b18b6689f5e822f09f1176788b580c2d1e7', dbname= 'd7dcs98v6bp756')
	cursor = conn.cursor()
	conn.autocommit=True
	#Para saber la cantidad de proveedores primer query
	cantidadproveedores = 'SELECT COUNT(id) FROM proveedor'
	cursor.execute(cantidadproveedores)
	conn.commit()

	#Guardar el resultado en una variable "devuelve un array o tupla"
	fa = cursor.fetchall()
	cantidadpesos = 'SELECT COUNT(id_peso) FROM peso_automatico'
	cursor.execute(cantidadpesos)
	conn.commit()
	peso = cursor.fetchall()
	fi = int(np.random.uniform(fa, fa))
	hoy = date.today()
	estado = 'materiaprima'
	while(repetir>0):
		repetir=repetir-1
		hoy = hoy + timedelta(days=7)
		time.sleep(10)
		for i in range(1,fi+1):
			#Simula el peso que debe generar el peso
			cantidadTonelada = int(np.random.normal(meanTonelada, stdTonelada))
			query = "SELECT fk_mp FROM proveedor WHERE id=%s"
			cursor.execute(query, (i,))
			conn.commit()
			fe = (cursor.fetchall())
			#Para saber el proveedor
			fk_MP = int(np.random.uniform(fe, fe))
			#Id del peso que ejecuto el envio
			peso_aut = int(np.random.uniform(1, peso))
			precioTotal=cantidadTonelada*precioOroKg
			payload = {
					"fecha": str(hoy + timedelta(days=np.random.uniform(0,6))),
					"cantidad": str(cantidadTonelada),
					"proveedor": str(i),
					"mp": str(fk_MP),
					"peso": str(peso_aut),
					"est": str(estado),
					"costototal": str(precioTotal)

				}
			client.publish('unimet/admin/bd',json.dumps(payload),qos=0)		
			print(payload)
			time.sleep(2)
			cantidadTonelada = int(np.random.normal(meanTonelada, stdTonelada))
			peso_aut = int(np.random.uniform(1, peso))
			precioTotal=cantidadTonelada*precioOroKg
			payload1 = {
					"fecha": str(hoy + timedelta(days=np.random.uniform(0,6))),
					"cantidad": str(cantidadTonelada),
					"proveedor": str(i),
					"mp": str(fk_MP),
					"peso": str(peso_aut),
					"est": str(estado),
					"costototal": str(precioTotal)

				}
			client.publish('unimet/admin/bd',json.dumps(payload1),qos=0)		
			print(payload1)
			time.sleep(2)

if __name__ == '__main__':
	main()
	sys.exit(0)