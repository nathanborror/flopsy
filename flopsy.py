import simplejson
import uuid

from amqplib import client_0_8 as amqp

DEFAULT_HOST = '127.0.0.1'
DEFAULT_USER_ID = 'guest'
DEFAULT_PASSWORD = 'guest'
DEFAULT_VHOST = '/'
DEFAULT_PORT = 5672
DEFAULT_INSIST = False
DEFAULT_QUEUE = 'default_queue'
DEFAULT_ROUTING_KEY = 'default_routing_key'
DEFAULT_EXCHANGE = 'default_exchange'
DEFAULT_DURABLE = True
DEFAULT_EXCLUSIVE = False
DEFAULT_AUTO_DELETE = False
DEFAULT_DELIVERY_MODE = 2
try:
    from django.conf import settings
    DEFAULT_HOST = getattr(settings, 'AMQP_SERVER', DEFAULT_HOST)
    DEFAULT_USER_ID = getattr(settings, 'AMQP_USER', DEFAULT_USER_ID)
    DEFAULT_PASSWORD = getattr(settings, 'AMQP_PASSWORD', DEFAULT_PASSWORD)
    DEFAULT_VHOST = getattr(settings, 'AMQP_VHOST', DEFAULT_VHOST)
    DEFAULT_PORT = getattr(settings, 'AMQP_PORT', DEFAULT_PORT)
    DEFAULT_INSIST = getattr(settings, 'AMQP_INSIST', DEFAULT_INSIST)
    DEFAULT_QUEUE = getattr(settings, 'AMQP_QUEUE', DEFAULT_QUEUE)
    DEFAULT_ROUTING_KEY = getattr(settings, 'AMQP_ROUTING_KEY', DEFAULT_ROUTING_KEY)
    DEFAULT_EXCHANGE = getattr(settings, 'AMQP_EXCHANGE', DEFAULT_EXCHANGE)
    DEFAULT_DURABLE = getattr(settings, 'AMQP_DURABLE', DEFAULT_DURABLE)
    DEFAULT_EXCLUSIVE = getattr(settings, 'AMQP_EXCLUSIVE', DEFAULT_EXCLUSIVE)
    DEFAULT_AUTO_DELETE = getattr(settings, 'AMQP_AUTO_DELETE', DEFAULT_AUTO_DELETE)
    DEFAULT_DELIVERY_MODE = getattr(settings, 'AMQP_DELIVERY_MODE', DEFAULT_DELIVERY_MODE)
except ImportError:
    pass


class Connection(object):
    def __init__(self, host=DEFAULT_HOST, user_id=DEFAULT_USER_ID,
        password=DEFAULT_PASSWORD, vhost=DEFAULT_VHOST, port=DEFAULT_PORT,
        insist=DEFAULT_INSIST):

        self.host = host
        self.user_id = user_id
        self.password = password
        self.vhost = vhost
        self.port = port
        self.insist = insist

        self.connect()

    def connect(self):
        self.connection = amqp.Connection(
            host='%s:%s' % (self.host, self.port),
            userid=self.user_id,
            password=self.password,
            virtual_host=self.vhost,
            insist=self.insist
        )


class Consumer(object):
    def __init__(self, routing_key=DEFAULT_ROUTING_KEY,
        exchange=DEFAULT_EXCHANGE, queue=DEFAULT_QUEUE,
        durable=DEFAULT_DURABLE, exclusive=DEFAULT_EXCLUSIVE,
        auto_delete=DEFAULT_AUTO_DELETE, connection=None):

        self.callbacks = {}

        self.routing_key = routing_key
        self.exchange = exchange
        self.queue = queue
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.connection = connection or Connection()
        self.channel = self.connection.connection.channel()

        self.channel.queue_declare(
            queue=self.queue,
            durable=self.durable,
            exclusive=self.exclusive,
            auto_delete=self.auto_delete
        )
        self.channel.exchange_declare(
            exchange=self.exchange,
            type='direct',
            durable=self.durable,
            auto_delete=self.auto_delete
        )
        self.channel.queue_bind(
            queue=self.queue,
            exchange=self.exchange,
            routing_key=self.routing_key
        )
        self.channel.basic_consume(
            queue=self.queue,
            no_ack=True,
            callback=self.dispatch,
            consumer_tag=str(uuid.uuid4())
        )

    def close(self):
        if getattr(self, 'channel'):
            self.channel.close()
        if getattr(self, 'connection'):
            self.connection.close()

    def wait(self):
        while True:
            self.channel.wait()

    def dispatch(self, message):
        decoded = simplejson.loads(message.body)
        message.body = decoded['data']
        callback = self.callbacks.get(decoded['kind'])
        if callback:
            callback(message)

    def register(self, kind, callback):
        self.callbacks[kind] = callback

    def unregister(self, kind):
        del self.callbacks[kind]


class Publisher(object):
    def __init__(self, routing_key=DEFAULT_ROUTING_KEY,
        exchange=DEFAULT_EXCHANGE, connection=None,
        delivery_mode=DEFAULT_DELIVERY_MODE):

        self.connection = connection or Connection()
        self.channel = self.connection.connection.channel()
        self.exchange = exchange
        self.routing_key = routing_key
        self.delivery_mode = delivery_mode

    def publish(self, kind, message_data):
        encoded = simplejson.dumps({'kind': kind, 'data': message_data})
        message = amqp.Message(encoded)
        message.properties['delivery_mode'] = self.delivery_mode
        self.channel.basic_publish(
            message,
            exchange=self.exchange,
            routing_key=self.routing_key
        )
        return message

    def close(self):
        if getattr(self, 'channel'):
            self.channel.close()
        if getattr(self, 'connection'):
            self.connection.connection.close()