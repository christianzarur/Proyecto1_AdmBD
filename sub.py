import ssl
import sys
import psycopg2
import paho.mqtt.client
import json

#Conexion BD remota, la cambie a localhost, lento el servidor
#conn = psycopg2.connect(host = 'ec2-107-20-167-11.compute-1.amazonaws.com', user= 'inytxpqzmlwhkp', password ='167ff1fd25c60913b54178a1c3427b18b6689f5e822f09f1176788b580c2d1e7', dbname= 'd7dcs98v6bp756')
#Conexion local
conn = psycopg2.connect(host='localhost', user='postgres',
                        password='toby3030', dbname='cerveceria')


def doQuery(a):
    cur = conn.cursor()
    cur.execute('INSERT INTO lote ("cantidadenviada", "fecha", "fkproveedor", "fkmp", "estado", "precio$") VALUES (%s, %s, %s, %s, %s, %s );',
                (a["cantidad"], a["fecha"], a["proveedor"], a["mp"], a["est"], a["costototal"]))
    conn.commit()


def on_connect(client, userdata, flags, rc):
    print('connected (%s)' % client._client_id)
    client.subscribe(topic='unimet/#', qos=0)


def on_message(client, userdata, message):
    a = json.loads(message.payload)
    print(a)
    print(message.qos)
    print('------------------------------')
    doQuery(a)


def main():
	client = paho.mqtt.client.Client()
	client.on_connect = on_connect
	client.message_callback_add('unimet/admin/bd', on_message)
	client.connect(host='localhost')
	client.loop_forever()


if __name__ == '__main__':
	main()
	sys.exit(0)
