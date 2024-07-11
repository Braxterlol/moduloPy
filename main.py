import pika
import numpy as np
import pandas as pd


def process_sensor_data(body):
    # si vienen en formato CSV
    data = body.decode('utf-8')
    df = pd.read_csv(pd.compat.StringIO(data))

   # ejemplo de calculos
    stats = {
        'temperature_mean': np.mean(df['temperature']),
        'temperature_std': np.std(df['temperature']),
        'humidity_mean': np.mean(df['humidity']),
        'humidity_std': np.std(df['humidity']),
    }

    print("Estadísticas calculadas:", stats)
    # se puede continuar...


def callback():
    print("Mensaje recibido...")

def main():
    # Conexión a RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('url_rabbit'))
    channel = connection.channel()

    # Asegúrate de que la cola exista
    channel.queue_declare(queue='Nombre_de_la_cola')

    # Escucha la cola
    channel.basic_consume(queue='Nombre_de_la_cola', on_message_callback=callback, auto_ack=False)

    print('Esperando mensajes...')
    channel.start_consuming()

if __name__ == "__main__":
    main()
