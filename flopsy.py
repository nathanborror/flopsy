from django.conf import settings
from amqplib import client_0_8 as amqp


class Connection(object):
    def __init__(self, *args, **kwargs):

        self.host = kwargs.get('host', getattr(settings, 'AMQP_SERVER'))
        self.user_id = kwargs.get('user_id', getattr(settings, 'AMQP_USER'))
        self.password = kwargs.get('password', getattr(settings, 'AMQP_PASSWORD'))
        self.vhost = kwargs.get('vhost', getattr(settings, 'AMQP_VHOST', '/'))
        self.port = kwargs.get('port', getattr(settings, 'AMQP_PORT', 5672))
        self.insist = False

        self.connect()

    def connect(self):
        self.connection = amqp.Connection(host='%s:%s' % (self.host, self.port), userid=self.user_id,
                password=self.password, virtual_host=self.vhost, insist=self.insist)


class Consumer(object):
    def __init__(self, connection):
        self.connection = connection
        self.channel = self.connection.connection.channel()

    def close(self):
        if getattr(self, 'channel'):
            self.channel.close()
        if getattr(self, 'connection'):
            self.connection.close()

    def declare(self, queue, exchange, routing_key, durable=True, exclusive=False, auto_delete=False):
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key

        self.channel.queue_declare(queue=self.queue, durable=durable,
                exclusive=exclusive, auto_delete=auto_delete)
        self.channel.exchange_declare(exchange=self.exchange, type='direct',
                durable=durable, auto_delete=auto_delete)
        self.channel.queue_bind(queue=self.queue, exchange=self.exchange,
                routing_key=self.routing_key)

    def wait(self):
        while True:
            self.channel.wait()

    def register(self, callback, queue=None, consumer_tag='flopsy_callback'):
        if hasattr(self, 'queue') or queue:
            self.consumer_tag = consumer_tag
            self.channel.basic_consume(queue=getattr(self, 'queue', queue), no_ack=True, 
                callback=callback, consumer_tag=consumer_tag)

    def unregister(self, consumer_tag='flopsy_callback'):
        self.channel.basic_cancel(consumer_tag)


class Publisher(object):
    def __init__(self, connection, exchange, routing_key, delivery_mode=2):
        self.connection = connection
        self.channel = self.connection.connection.channel()
        self.exchange = exchange
        self.routing_key = routing_key
        self.delivery_mode = delivery_mode

    def publish(self, message_data):
        message = amqp.Message(message_data)
        message.properties['delivery_mode'] = self.delivery_mode
        self.channel.basic_publish(message, exchange=self.exchange, routing_key=self.routing_key)
        return message

    def close(self):
        if getattr(self, 'channel'):
            self.channel.close()
        if getattr(self, 'connection'):
            self.connection.connection.close()