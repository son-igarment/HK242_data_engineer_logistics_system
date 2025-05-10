import pika
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    # RabbitMQ connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv('RABBITMQ_HOST', 'localhost'),
            port=int(os.getenv('RABBITMQ_PORT', 5672)),
            credentials=pika.PlainCredentials(
                os.getenv('RABBITMQ_USER', 'user'),
                os.getenv('RABBITMQ_PASSWORD', 'password')
            )
        )
    )
    channel = connection.channel()

    # Declare exchange
    channel.exchange_declare(exchange='vehicle_updates', exchange_type='fanout')

    # Declare queue for backend
    channel.queue_declare(queue='backend_queue')
    channel.queue_bind(exchange='vehicle_updates', queue='backend_queue')

    print("RabbitMQ server is running...")
    try:
        connection.process_data_events(time_limit=None)
    except KeyboardInterrupt:
        print("Stopping RabbitMQ server...")
    finally:
        connection.close()

if __name__ == "__main__":
    main() 