import pika
import json
import requests
import time
import numpy as np
import pandas as pd
from collections import defaultdict

class DataProcessor:
    def __init__(self):
        self.data = defaultdict(list)

    def add_data(self, new_data):
        for key, value in new_data.items():
            if isinstance(value, (int, float)):
                self.data[key].append(value)

    def calculate_statistics(self):
        stats = {}
        for key, values in self.data.items():
            if len(values) > 0:
                stats[key] = {
                    'mean': np.mean(values),
                    'median': np.median(values),
                    'std': np.std(values),
                    'min': np.min(values),
                    'max': np.max(values)
                }
        return stats

    def generate_distribution_table(self, column, bins=10):
        if column in self.data and len(self.data[column]) > 0:
            series = pd.Series(self.data[column])
            return pd.cut(series, bins=bins).value_counts().sort_index()
        return None

data_processor = DataProcessor()

def connect():
    retry_interval = 5000  

    while True:
        try:
            connection = pika.BlockingConnection(pika.URLParameters('amqp://plantcare-rabbit.integrador.xyz/'))
            channel = connection.channel()

            queue_name = 'sensores'
            channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})

            print(f"Esperando mensajes en la cola {queue_name}...")

            def callback(ch, method, properties, body):
                try:
                    print("Mensaje recibido:", body.decode())
                    
                    parsed_content = json.loads(body.decode())
                    
                    data = {
                        "humidity_earth": parsed_content["soil_moisture"],
                        "humidity_environment": parsed_content["humidity"],
                        "mq135": parsed_content["mq135_2"],
                        "brightness": parsed_content["ldr"],
                        "ambient_temperature": parsed_content["temperature"],
                        "estado": 0,
                        "mac": parsed_content["mac_address"],
                    }
                    
                    data_processor.add_data(data)
                    
                    enviar_mensaje('http://44.197.7.97:8081/api/plantRecord', data)
                    
                    # Calcular y mostrar estadísticas cada 10 mensajes
                    if len(data_processor.data['humidity_earth']) % 10 == 0:
                        print("\nEstadísticas actualizadas:")
                        print(json.dumps(data_processor.calculate_statistics(), indent=2))
                        
                        print("\nTabla de distribución para humidity_earth:")
                        print(data_processor.generate_distribution_table('humidity_earth'))
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as error:
                    print("Error al procesar el mensaje:", str(error))
                    ch.basic_nack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)

            channel.start_consuming()

        except Exception as error:
            print('Error al conectar con RabbitMQ:', str(error))
            print(f"Reintentando en {retry_interval / 1000} segundos...")
            time.sleep(retry_interval / 1000)

def enviar_mensaje(url, data):
    headers = {
        'Content-Type': 'application/json'
    }
    print(data)
    body = json.dumps(data)
    
    try:
        response = requests.post(url, headers=headers, data=body)
        response_body = response.text
        if response.ok:
            print("Mensaje enviado correctamente.")
        else:
            print(f"Error al enviar el mensaje: {response.status_code} {response.reason}")
            print(f"Cuerpo de la respuesta: {response_body}")
    except Exception as error:
        print(f"Error al enviar el mensaje: {str(error)}")

if __name__ == "__main__":
    connect()

    
