# -*- coding: utf-8 -*-

"""

    Module :mod:``

    This Module is created to...

    LICENSE: The End User license agreement is located at the entry level.

"""

# ----------- START: Native Imports ---------- #
import json
import uuid
# ----------- END: Native Imports ---------- #

# ----------- START: Third Party Imports ---------- #
import pika
# ----------- END: Third Party Imports ---------- #

# ----------- START: In-App Imports ---------- #
# ----------- END: In-App Imports ---------- #

__all__ = [
    # All public symbols go here.
]


class RmqRpc(object):
    def __init__(self):
        self.create_connection()
        self.create_channel()

    def create_connection(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )

    def create_channel(self):
        self.channel = self.connection.channel()

    def consume(self):
        self.channel.basic_consume(
            self.on_response, no_ack=True, queue=self.callback_queue
        )

    def declare_callback(self):
        self.callback_queue = self.channel.queue_declare(exclusive=True).method.queue


class RpcServer(RmqRpc):
    def __init__(self):
        super(self.__class__, self).__init__()

    def on_request(self, channel, method, props, body):

        response = "Processed Event: {} Successfully".format(body)
        print 'Received Event {}'.format(body)

        if getattr(props, 'reply_to', None) and props.reply_to:
            channel.basic_publish(
                exchange='',
                routing_key=props.reply_to,
                properties=pika.BasicProperties(correlation_id=props.correlation_id),
                body=str(response)
            )

            channel.basic_ack(delivery_tag=method.delivery_tag)

    def __call__(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.on_request, queue='rpc_queue')

        self.channel.start_consuming()

class RpcClient(RmqRpc):
    def __init__(self):
        super(self.__class__, self).__init__()
        self.declare_callback()
        self.consume()

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def __call__(self, payload):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue',
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=str(payload))
        while self.response is None:
            self.connection.process_data_events()
        return self.response

def event_emittor():
    emittor = RpcClient()

    while True:
        import time
        time.sleep(0.2)
        _id = str(uuid.uuid4())
        print '>>> Emiting', _id
        print emittor('{}'.format(_id))

def event_listener():
    listener = RpcServer()
    listener()

