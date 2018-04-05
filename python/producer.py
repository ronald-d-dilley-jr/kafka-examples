#!/usr/bin/env python3


import os
import sys
import logging
import time
import json
import random
import threading
import multiprocessing
from argparse import ArgumentParser
from collections import namedtuple


from kafka import KafkaProducer


logger = None
SYSTEM = 'KAFKA-DEMO'
COMPONENT = 'work-producer'


class Producer(threading.Thread):
    def __init__(self, cfg):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

        self.broker = cfg.kafka_info.broker
        self.topic = cfg.kafka_info.topic
        self.client_id = cfg.kafka_info.client_id

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers=self.broker,
                                 client_id=self.client_id)

        order_id = 1
        while not self.stop_event.is_set():
            for unit_id in range(1, random.randint(3, 6)):
                payload = dict()
                payload['order_id'] = '{0:>020}_{1:>05}'.format(order_id, unit_id)

                producer.send(self.topic,
                              value=json.dumps(payload).encode('utf-8'),
                              partition=random.randint(0, 2))
            time.sleep(1)
            order_id += 1

        producer.close()


class LoggingFilter(logging.Filter):
    """Our logging filter
    """

    def __init__(self, system='', component=''):
        super(LoggingFilter, self).__init__()

        self.system = system
        self.component = component

    def filter(self, record):
        record.system = self.system
        record.component = self.component

        return True


class ExceptionFormatter(logging.Formatter):
    """Standard logging formatter with special execption formatting
    """

    def __init__(self, fmt=None, datefmt=None):
        std_fmt = ('%(asctime)s.%(msecs)03d'
                   ' %(levelname)-8s'
                   ' %(system)s'
                   ' %(component)s'
                   ' %(message)s')
        std_datefmt = '%Y-%m-%dT%H:%M:%S'

        if fmt is not None:
            std_fmt = fmt

        if datefmt is not None:
            std_datefmt = datefmt

        super(ExceptionFormatter, self).__init__(fmt=std_fmt,
                                                 datefmt=std_datefmt)

    def formatException(self, exc_info):
        result = super(ExceptionFormatter, self).formatException(exc_info)
        return repr(result)

    def format(self, record):
        s = super(ExceptionFormatter, self).format(record)
        if record.exc_text:
            s = s.replace('\n', ' ')
            s = s.replace('\\n', ' ')
        return s


def setup_logging(cfg):
    """Configure the message logging components
    """

    global logger

    # Setup the logging level
    logging_level = logging.INFO
    if cfg.debug:
        logging_level = cfg.debug

    handler = logging.StreamHandler(sys.stdout)
    msg_formatter = ExceptionFormatter()
    msg_filter = LoggingFilter(SYSTEM, COMPONENT)

    handler.setFormatter(msg_formatter)
    handler.addFilter(msg_filter)

    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.addHandler(handler)


def retrieve_command_line():
    """Read and return the command line arguments
    """

    description = 'Kafka-Demo Work-Consumer'
    parser = ArgumentParser(description=description)

    parser.add_argument('--broker',
                        action='store',
                        dest='broker',
                        required=False,
                        default='localhost:9092',
                        help='Kafka broker to connect to')

    parser.add_argument('--topic',
                        action='store',
                        dest='topic',
                        required=True,
                        help='Kafka topic to use consume')

    parser.add_argument('--client-id',
                        action='store',
                        dest='client_id',
                        required=True,
                        help='Kafka client ID to use')

    parser.add_argument('--partition',
                        action='store',
                        dest='partition',
                        required=True,
                        type=int,
                        help='Kafka partition to use')

    parser.add_argument('--debug',
                        action='store',
                        dest='debug',
                        required=False,
                        type=int,
                        default=0,
                        choices=[0,10,20,30,40,50],
                        metavar='DEBUG_LEVEL',
                        help='Log debug messages')

    return parser.parse_args()


def get_env_var(variable, default):
    result = os.environ.get(variable, default)
    if not result:
        raise RuntimeError('You must specify {} in the environment'
                           .format(variable))
    return result


KafkaInfo = namedtuple('KafkaInfo',
                       ('broker', 'topic', 'partition', 'client_id'))

ConfigInfo = namedtuple('ConfigInfo',
                        ('kafka_info', 'debug'))


def get_configuration():

    args = retrieve_command_line()

    kafka_info = KafkaInfo(broker=args.broker,
                           topic=args.topic,
                           partition=args.partition,
                           client_id=args.client_id)

    config_info = ConfigInfo(kafka_info=kafka_info,
                             debug=args.debug)

    return config_info


def main():

    cfg = get_configuration()
    setup_logging(cfg)

    tasks = [
        Producer(cfg)
    ]

    for t in tasks:
        t.start()

    time.sleep(20)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == '__main__':

    random.seed(time.gmtime())

    main()
