import pika
import sys
 
class ServiceB:
    def __init__(self, rabbitmq_host='localhost'):
        self.rabbitmq_host = rabbitmq_host
        self.queue_1 = 'Queue-1'
        self.queue_2 = 'Queue-2'
 
        self.connection, self.channel = self.establish_connection()
 
    def establish_connection(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_host))
        channel = connection.channel()
        channel.queue_delete(queue=self.queue_1)
        channel.queue_delete(queue=self.queue_2)
        return connection, channel
 
    def consume_from_queue_1(self):
        method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_1, auto_ack=True)
        if method_frame:
            print(f"Service B received: {body.decode()}")
            return int(body.decode())
        else:
            return None
 
    def produce_to_queue_2(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_2, body=str(message),
                                   properties=pika.BasicProperties(delivery_mode=2))
        print(f"Service B produced: {message}")
 
    def process(self):
        # Ensure the queues are durable
        self.channel.queue_declare(queue=self.queue_1, durable=True)
        self.channel.queue_declare(queue=self.queue_2, durable=True)
 
        while True:
            # Consume from Queue-1 (blocking)
            X = None
            while X is None:
                X = self.consume_from_queue_1()
 
            if X == -1:
                break
 
            # Perform the square operation
            X = X ** 2
            print(f"Service B squared the number: {X}")
 
            # Produce to Queue-2
            self.produce_to_queue_2(X)
 
        self.connection.close()
 
 
if __name__ == "__main__":
    service_b = ServiceB()
    service_b.process()