import uuid

import opentracing
from django.shortcuts import render
from django.views import generic

import pika
import pika.spec
from opentracing import tags

from rpc import common

tracer = common.initialize_tracer('producer')


class IndexView(generic.TemplateView):
    template_name = 'index.html'

    def __init__(self):
        super().__init__()
        self.rpc_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=common.BROKER['host'], port=common.BROKER['port'])
        )
        self.rpc_channel = self.rpc_connection.channel()
        self.rpc_callback_queue = self.rpc_channel.queue_declare(queue='', exclusive=True).method.queue
        self.rpc_channel.basic_consume(
            queue=self.rpc_callback_queue,
            on_message_callback=self.on_rpc_response,
            auto_ack=True
        )
        self.rpc_channel.queue_declare(queue=common.BROKER['queue'], durable=True)
        self.rpc_correlation_id = str(uuid.uuid4())
        self.rpc_response = None

    def on_rpc_response(self, ch, method, props, body):
        if self.rpc_correlation_id == props.correlation_id:
            self.rpc_response = body

    def post(self, request, *args, **kwargs):
        span_tags = {
            tags.HTTP_URL: request.path,
            tags.HTTP_METHOD: request.method,
            tags.SERVICE: request.resolver_match.view_name,
            tags.MESSAGE_BUS_DESTINATION: self.rpc_correlation_id,
            tags.SPAN_KIND: tags.SPAN_KIND_RPC_CLIENT
        }
        with tracer.start_active_span('rpc_client -> rpc_server', tags=span_tags) as scope:
            carrier = {}
            tracer.inject(scope.span, opentracing.Format.TEXT_MAP, carrier)
            self.rpc_channel.basic_publish(
                exchange='',
                routing_key=common.BROKER['queue'],
                properties=pika.BasicProperties(
                    headers=carrier,
                    reply_to=self.rpc_callback_queue,
                    correlation_id=self.rpc_correlation_id,
                ),
                body='request_{}'.format(self.rpc_correlation_id)
            )
            while self.rpc_response is None:
                self.rpc_connection.process_data_events()
            context = {'response': self.rpc_response.decode()}
            self.rpc_connection.close()
            return render(request, self.template_name, context)

    def get(self, request, *args, **kwargs):
        # common.compute()  # TODO comment me out to enable perfomances drop
        return super().get(request, *args, **kwargs)
