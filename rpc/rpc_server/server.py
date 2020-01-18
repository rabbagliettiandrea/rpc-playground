import opentracing
from opentracing import tags

import pika

from rpc import common

tracer = common.initialize_tracer('worker')


def _receive_callback(ch, method, properties, body):
    span_ctx = tracer.extract(opentracing.Format.TEXT_MAP, properties.headers)
    span_tags = {
        tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER,
        tags.SERVICE: 'worker',
        tags.MESSAGE_BUS_DESTINATION: properties.correlation_id
    }
    with tracer.start_active_span('rpc_server -> rpc_client', child_of=span_ctx, tags=span_tags):
        print(' [x] Received "{}"'.format(body.decode()))
        print(' [x] Processing "{}"'.format(body.decode()))
        common.compute()
        ch.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
            body='response_{}'.format(properties.correlation_id)
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(' [x] Done "{}"'.format(body.decode()))


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=common.BROKER['host'], port=common.BROKER['port'])
    )
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(queue=common.BROKER['queue'], durable=True)
    channel.basic_consume(queue=common.BROKER['queue'], on_message_callback=_receive_callback)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    main()
    exit(0)
