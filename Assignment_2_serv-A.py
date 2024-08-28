import pika
import sys
 
 
class ServiceA:
    def __init__(self, rabbitmq_host='localhost'):
        self.rabbitmq_host = rabbitmq_host
        self.queue_1 = 'Queue-1'
        self.queue_2 = 'Queue-2'
        self.A = 2
        self.B = 6
 
        self.connection, self.channel = self.establish_connection()
 
    def establish_connection(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_host))
        channel = connection.channel()
        channel.queue_delete(queue=self.queue_1)
        channel.queue_delete(queue=self.queue_2)
        return connection, channel
 
    def produce_to_queue_1(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_1, body=str(message),
                                   properties=pika.BasicProperties(delivery_mode=2))
        print(f"Service A produced: {message}")
 
    def consume_from_queue_2(self):
        method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_2, auto_ack=True)
        if method_frame:
            print(f"Service A received: {body.decode()}")
            return int(body.decode())
        else:
            return None
 
    def process(self, max_iterations):
        # Ensure the queues are durable
        self.channel.queue_declare(queue=self.queue_1, durable=True)
        self.channel.queue_declare(queue=self.queue_2, durable=True)
 
        # Initial production
        X = self.A * self.B
        print(f"Initial X = A * B = {X}")
        self.produce_to_queue_1(X)
 
        for _ in range(max_iterations):
            # Consume from Queue-2 (blocking)
            X = None
            while X is None:
                X = self.consume_from_queue_2()
 
            # Perform the multiplication
            X *= self.B
            print(f"Service A multiplying with B={self.B}: {X}")
 
            # Produce to Queue-1
            self.produce_to_queue_1(X)
 
        # Signal to stop Service B
        self.produce_to_queue_1(-1)
 
        self.connection.close()
 
 
if __name__ == "__main__":
    if len(sys.argv) > 1:
        max_iterations = int(sys.argv[1])
    else:
        max_iterations = int(input("Enter maximum iterations: "))
 
    service_a = ServiceA()
    service_a.process(max_iterations)