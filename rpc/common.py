from jaeger_client import Config
import time
from random import randint, random

BROKER = {
    'host': 'localhost',
    'port': 5680,
    'queue': 'rpc'
}


def initialize_tracer(service_name):
    _jaeger_config = Config(
        config={
            'sampler': {
                'type': 'const',
                'param': 1,
            }
        },
        service_name='rpc-{}'.format(service_name)
    )
    tracer = _jaeger_config.initialize_tracer()
    return tracer


def compute():
    time.sleep(randint(1, 2) + random())
