#!/usr/bin/env python3


import os
import sys
import logging
import time
import random
from argparse import ArgumentParser
from collections import namedtuple


from kafka import KafkaConsumer
from kafka import TopicPartition


logger = None
SYSTEM = 'KAFKA-DEMO'
COMPONENT = 'work-consumer'


class Consumer():
    def __init__(self, cfg):

        self.broker = cfg.kafka_info.broker
        self.topic = cfg.kafka_info.topic
        self.client_id = cfg.kafka_info.client_id
        self.group_id = cfg.kafka_info.group_id
        self.partition = cfg.kafka_info.partition

    def consume(self):

        consumer = KafkaConsumer(bootstrap_servers=self.broker,
                                 client_id=self.client_id,
                                 group_id=self.group_id)

        if self.partition is None:
            consumer.subscribe([self.topic])
        else:
            tp = TopicPartition(self.topic, int(self.partition))
            consumer.assign([tp])
            #logger.info(consumer.committed(tp))
            #logger.info(consumer.position(tp))

        try:
            while True:
                for message in consumer:
                    logger.info('{} {} {} {} {}'.format(message.topic,
                                                        message.partition,
                                                        message.offset,
                                                        message.key,
                                                        message.value.decode()))
                    #time.sleep(random.randint(3, 20))
                    time.sleep(2)
#                    if message:
#                        for key in message:
#                            #print(type(key))
#                            #print(key.topic,
#                            #       key.partition)
#                            for value in message[key]:
#                                #print(type(value))
#                                print(value.topic,
#                                      value.partition,
#                                      value.offset,
#                                      value.value.decode())
                else:
                    time.sleep(1)
        except KeyboardInterrupt:
            pass

        consumer.close()


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

    parser.add_argument('--partition',
                        action='store',
                        dest='partition',
                        required=False,
                        default=None,
                        help='Kafka partition to use')

    parser.add_argument('--client-id',
                        action='store',
                        dest='client_id',
                        required=True,
                        help='Kafka client ID to use')

    parser.add_argument('--group-id',
                        action='store',
                        dest='group_id',
                        required=True,
                        help='Kafka group ID to use')

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
                       ('broker', 'topic', 'partition', 'client_id', 'group_id'))

ConfigInfo = namedtuple('ConfigInfo',
                        ('kafka_info', 'debug'))


def get_configuration():

    args = retrieve_command_line()

    kafka_info = KafkaInfo(broker=args.broker,
                           topic=args.topic,
                           partition=args.partition,
                           client_id=args.client_id,
                           group_id=args.group_id)

    config_info = ConfigInfo(kafka_info=kafka_info,
                             debug=args.debug)

    return config_info


def main():

    cfg = get_configuration()
    setup_logging(cfg)

    task = Consumer(cfg)

    task.consume()


if __name__ == '__main__':

    random.seed(time.gmtime())

    main()
