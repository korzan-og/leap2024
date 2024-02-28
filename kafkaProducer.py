#!/usr/bin/env python

import sys
from random import choice
from time import sleep
from argparse import ArgumentParser, FileType
# from configparser import ConfigParser
from confluent_kafka import Producer, avro
from confluent_kafka.avro import AvroProducer
import logging
import os
import configparser
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from uuid import uuid4

configParser = configparser.RawConfigParser()   
configFilePath = r'config.ini'
configParser.read(configFilePath)

username=configParser.get('confluent', 'sasl.username')
password=configParser.get('confluent', 'sasl.password')
mechanism=configParser.get('confluent', 'sasl.mechanisms')
broker=configParser.get('confluent', 'bootstrap.servers')
protocol=configParser.get('confluent', 'security.protocol')
clientId=configParser.get('confluent', 'client.id')
schemaUrl=configParser.get('confluent', 'schema.url')
schemaAuth=configParser.get('confluent', 'schema.auth')
lingerMs=configParser.get('confluent', 'linger.ms')
# TODO Add serialzier
# key_serializer=configParser.get('confluent', 'key_serializer_class_config')
# value_serializer=configParser.get('confluent', 'value_serializer_class_config')
config={'bootstrap.servers': broker,'security.protocol':protocol,'sasl.mechanisms':mechanism,'client.id':clientId,'sasl.username':username,'sasl.password':password,'linger.ms':lingerMs} # For cloud

logLevel=configParser.get('logging', 'logging.level')
logging.basicConfig()
logging.getLogger().setLevel(logLevel)

class newData(object):
    def __init__(self, version, inputs):
        self.version = version
        self.inputs = inputs  

def newData_to_dict(newData, ctx):
    # User._address must not be serialized; omit from dict
    return dict(version=newData.version,
                inputs=newData.inputs)



def delivery_report(err, msg):
    if err is not None:
        logging.error("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    logging.info('new record: key {} version {} Avg_sal_L3M_ {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), str(msg.value()), str(msg.value()) ,msg.topic(), msg.partition(), msg.offset()))

def kafkaProducer(topic,key,message):
    producer = Producer(config)
    def delivery_callback(err, msg):
        if err:
            logging.error('ERROR: Message failed delivery failed for {}: {}'.format(topic, err))
        else:
            logging.info("Produced event to topic {topic}: key = {key} value = {value}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value()))
    producer.produce(topic, message, key, callback=delivery_callback)
    producer.poll(10000)


def kafkaAvroProducer(topic,key,message):
    # is_specific =  "true"

    # if is_specific:
    #     schema = "user_specific.avsc"
    # else:
    #     schema = "user_generic.avsc"

    # path = os.path.realpath(os.path.dirname(__file__))
    # with open(f"{path}/avro/{schema}") as f:
    #     schema_str = f.read()

    # schema_registry_conf = {'url': schemaUrl,'basic.auth.user.info':schemaAuth}
    # schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # avro_serializer = AvroSerializer(schema_registry_client,
    #                                  schema_str,
    #                                  newData_to_dict)

    # string_serializer = StringSerializer('utf_8')
    # producer = Producer(config)
    # logging.info("Producing newData to topic {}. ^C to exit.".format(topic))
    
    # while True:
    #     # Serve on_delivery callbacks from previous calls to produce()
    #     producer.poll(0.0)
    #     try:
    #         newMessage = newData(version=message['version'],
    #                     inputs=message['inputs'])
    #         producer.produce(topic=topic,
    #                          key=string_serializer(str(key)),
    #                          value=avro_serializer(newMessage, SerializationContext(topic, MessageField.VALUE)),
    #                          on_delivery=delivery_report)
    #         # producer.flush()
    #     except KeyboardInterrupt:
    #         break
    #     except ValueError:
    #         logging.warn("Invalid input, discarding record...")
    #         continue

    # logging.info("\nFlushing records...")
    # producer.flush()

    value_schema = avro.load('avro/produce.avsc')
    key_schema = avro.load('avro/KeySchema.avsc')
    avroProducer = AvroProducer(
    {'bootstrap.servers': broker,'security.protocol':protocol,'sasl.mechanisms':mechanism,'sasl.username':username,'sasl.password':password,'schema.registry.url': schemaUrl,'schema.registry.user.info':schemaAuth},default_key_schema=key_schema, default_value_schema=value_schema)
    for i in range(0, 100000):
        avroProducer.produce(topic=topic, value=message, key=key, key_schema=key_schema, value_schema=value_schema)
        sleep(0.01)
        print(i)

    avroProducer.flush(10)