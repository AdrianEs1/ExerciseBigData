# UNIVERSIDAD NACIONAL ABIERTA Y A DISTANCIA UNAD

#  Curso: BIGDATA
# Estudiantes: Adrian Armero, Jose Portilla, Leonardo Ramirez

#Este código es desarrollado para la generación de datos aleatorios sobre seguridad informatica
#se tienen en cuenta aspectos como la ip, navegador, nombre de ususario, entre otros,
#estos datos se envian al topic para luego ser consumidos

from kafka import KafkaProducer
from faker import Faker
import json
import random
import time

#Se crea una instancia de Faker para generar datos ficticios.
#Se configura el Kafka Producer, apuntando al servidor Kafka local (localhost:9092) y serializando los datos a JSON (convertidos a bytes).
 
fake = Faker()
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                     	value_serializer=lambda v: json.dumps(v).encode('utf-8'))
 
·         Listas con posibles tipos de eventos y dispositivos desde los cuales podrían generarse esos eventos.
event_types = ["LOGIN_FAILED", "LOGIN_SUCCESS", "ACCESS_GRANTED", "BLOCKED"]
devices = ["PC-Windows", "Laptop-Linux", "Android", "iPhone", "MacOS"]
 
#Esta función genera un evento aleatorio con:
#	Fecha/hora, IP, tipo de evento, usuario, y dispositivo.
 
def generarDato():
    	return event = {
    	"timestamp": fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"),
    	"ip": fake.ipv4(),
    	"event_type": random.choice(event_types),
    	"username": fake.user_name(),
    	"device": random.choice(devices)}
		
 
 
#En un bucle infinito:
#Se generan 10 eventos aleatorios.
#Se envían al topic seguridad_logs en Kafka.
#Se espera 2 segundos antes de generar el siguiente lote.
 

while True:
    for _ in range(10):
            event=generarDato()
            producer.send('seguridad_logs', event)
    print("Eviado: 10 eventos")
    time.sleep(2)  # cada dos segundo se simula diez  nuevos eventos


