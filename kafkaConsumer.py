#!/usr/bin/env python

import sys
from random import choice
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import logging
import configparser
import json
import time
import random
import os
from collections import defaultdict
import io
from confluent_kafka import Consumer, KafkaError
from avro.io import DatumReader, BinaryDecoder
import avro.schema
from app import process_msg

configParser = configparser.RawConfigParser()   
configFilePath = r'config.ini'
configParser.read(configFilePath)

logLevel=configParser.get('logging', 'logging.level')
logging.basicConfig()
logging.getLogger().setLevel(logLevel)
username=configParser.get('confluent', 'sasl.username')
password=configParser.get('confluent', 'sasl.password')
mechanism=configParser.get('confluent', 'sasl.mechanisms')
broker=configParser.get('confluent', 'bootstrap.servers')
protocol=configParser.get('confluent', 'security.protocol')
groupId=configParser.get('confluent', 'consumer.group.id')
autoOffsetReset=configParser.get('confluent', 'consumer.auto.offset.reset')
schemaUrl=configParser.get('confluent', 'schema.url')
schemaAuth=configParser.get('confluent', 'schema.auth')

# linkTopics=configParser.get('confluent', 'links.topic')

# TODO Add Deserializer
# key_deserializer=configParser.get('confluent', 'consumer.key_deserializer_class_config')
# value_deserializer=configParser.get('confluent', 'consumer.value_deserializer_class_config')

running = True
counter = 0
userId = 1

class Sas(object):
    def __init__(self, id, version, AVG_SAL_L3M=None,BUREAU_SCORE=None,CARD_TYPE=None,CREDIT_LIMIT=None,CURRENT_BALANCE_TO_CREDIT_LIMIT=None,DELINQUENT_INDICATOR=None,NUMBER_OF_CREDIT_CARDS=None,SALARY=None,TOT_CREDIT_CARD_BAL_L3M=None,TOT_CREDIT_CARD_BAL_L6M=None):
        self.id=id
        self.version=version
        self.AVG_SAL_L3M=AVG_SAL_L3M
        self.BUREAU_SCORE=BUREAU_SCORE
        self.CARD_TYPE=CARD_TYPE
        self.CREDIT_LIMIT=CREDIT_LIMIT
        self.CURRENT_BALANCE_TO_CREDIT_LIMIT=CURRENT_BALANCE_TO_CREDIT_LIMIT
        self.DELINQUENT_INDICATOR=DELINQUENT_INDICATOR
        self.NUMBER_OF_CREDIT_CARDS=NUMBER_OF_CREDIT_CARDS
        self.SALARY=SALARY
        self.TOT_CREDIT_CARD_BAL_L3M=TOT_CREDIT_CARD_BAL_L3M
        self.TOT_CREDIT_CARD_BAL_L6M=TOT_CREDIT_CARD_BAL_L6M
        



def dict_to_obj(obj, ctx):
    if obj is None:
        return None
    print("MESSAGE : " + str(obj))
    return Sas(id=userId,
                version=1,
                AVG_SAL_L3M=obj['AVG_SAL_L3M'],
                BUREAU_SCORE=obj['BUREAU_SCORE'],
                CARD_TYPE=obj['CARD_TYPE'],CREDIT_LIMIT=obj['CREDIT_LIMIT'],
                CURRENT_BALANCE_TO_CREDIT_LIMIT=obj['CUR_BALANCE_TO_CREDIT_LIMIT'],DELINQUENT_INDICATOR=obj['DELINQUENT_INDICATOR'],
                NUMBER_OF_CREDIT_CARDS=obj['NUMBER_OF_CREDIT_CARDS'],SALARY=obj['SALARY'],TOT_CREDIT_CARD_BAL_L3M=obj['TOT_CREDIT_CARD_BAL_L3M'],TOT_CREDIT_CARD_BAL_L6M=obj['TOT_CREDIT_CARD_BAL_L6M'])

def sas_to_request(obj):
    try:
        obj.AVG_SAL_L3M=float(obj.AVG_SAL_L3M)
    except ValueError:
       obj.AVG_SAL_L3M=random.uniform(15000.00000,25000.00000)

    try:
        obj.BUREAU_SCORE=float(obj.BUREAU_SCORE)
    except ValueError:
        obj.BUREAU_SCORE=random.uniform(450.0000000,650.0000000)
    try:
        obj.CREDIT_LIMIT=float(obj.CREDIT_LIMIT)
    except ValueError:
        obj.CREDIT_LIMIT=random.uniform(2000.00000,20000.000000)
    try:
        obj.CURRENT_BALANCE_TO_CREDIT_LIMIT=float(obj.CURRENT_BALANCE_TO_CREDIT_LIMIT)
    except ValueError:
        obj.CURRENT_BALANCE_TO_CREDIT_LIMIT=random.uniform(70.000000000,100.000000000)
    try:
        obj.NUMBER_OF_CREDIT_CARDS=int(obj.NUMBER_OF_CREDIT_CARDS)
    except ValueError:
        obj.NUMBER_OF_CREDIT_CARDS=random.uniform(1,10)
    try:
        obj.SALARY=float(obj.SALARY)
    except ValueError:
        obj.SALARY=random.uniform(20000.00000,30000.00000)
    try:
        obj.TOT_CREDIT_CARD_BAL_L3M=int(obj.TOT_CREDIT_CARD_BAL_L3M)
    except ValueError:
        obj.TOT_CREDIT_CARD_BAL_L3M=random.uniform(-15000,-1000)
    try:
        obj.TOT_CREDIT_CARD_BAL_L6M=int(obj.TOT_CREDIT_CARD_BAL_L6M)
    except ValueError:
        obj.TOT_CREDIT_CARD_BAL_L6M=random.uniform(-18000,-3000)
    newRequest = {}
    newInputs = []
    i = 0 
    params = ["AVG_SAL_L3M", "Bureau_Score", "card_type","Credit_Limit","Current_Balance_to_Credit_Limit","Delinquent_Indicator","Number_Of_Credit_Cards","salary","Tot_Credit_Card_Bal_L3M","Tot_Credit_Card_Bal_L6M"]
    for x in params:
        newInput = {}
        newInput['name']=x + "_"
        newInput['value']=getattr(obj, x.upper())
        newInputs.append(newInput)
    newRequest['version']=1
    newRequest['inputs']=newInputs
    logging.warn(str(newRequest))
    return newRequest
    

def kafkaConsumer(willCommit):
    startTime = time.time()
    topic=configParser.get('confluent', 'read.topic')
    logging.warn("Started consumer on " + str(startTime))
    counter = 0
    config={'bootstrap.servers': broker,'security.protocol':protocol, 'group.id':groupId,'sasl.mechanisms':mechanism,'sasl.username':username,'sasl.password':password, 'auto.offset.reset':autoOffsetReset,'enable.auto.offset.store': True, 'enable.auto.commit': True} # For cloud
    is_specific = "true"

    if is_specific:
        schema = "user_specific.avsc"
    else:
        schema = "user_generic.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()
    sr_conf = {'url': schemaUrl,'basic.auth.user.info':schemaAuth}
    schema_registry_client = SchemaRegistryClient(sr_conf)
    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_obj)
    
    consumer = Consumer(config, logger=logging)
    key = configParser.get('confluent', 'key')
    try:
        print("Consuming Topic  " + topic)
        consumer.subscribe([topic])
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if(counter>5):
                    shutdown()
                else:
                    counter = counter + 1
                    continue
            if msg.error():
                logging.error("ERROR: " + str(msg.error()))
                logging.error("Topic: " + str(msg.topic()))
                logging.error("Partition: " + str(msg.partition()))
                logging.error("Offset: " + str(msg.offset()))
                logging.error("Key: " + str(msg.key()))
            else:
                msg_value = msg.value()
                # event_dict = decode(msg_value)
                counter=0
                sas = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                # logging.info("Topic: " + str(msg.topic()))
                logging.info("NewRequest is here : ")
                # logging.info(str(sas_to_request(sas)))
                newRequest=sas_to_request(sas)
                # key=str(msg.offset())
                logging.info("Message #" + str(msg.offset()))
                # logging.info("version : "+ str(sas.version) + " inputs : "+str(sas.AVG_SAL_L3M) + " " + str(sas.BUREAU_SCORE)+ " " + str(sas.CARD_TYPE)+ " " + str(sas.CREDIT_LIMIT)+ " " + str(sas.CURRENT_BALANCE_TO_CREDIT_LIMIT)+ " " + str(sas.DELINQUENT_INDICATOR))
                process_msg(str(key),newRequest)
                if(willCommit==True):
                    consumer.commit(asynchronous=willCommit)
                key = key+1
            # userId = userId + 1
    finally:
        endTime = time.time()
        logging.warn("Closed consumer on " + str(endTime))
        logging.warn("Total runTime " + str(endTime-startTime))
        consumer.close()

def shutdown():
    running = False

# def decode(msg_value):
#     message_bytes = io.BytesIO(msg_value)
#     decoder = BinaryDecoder(message_bytes)
#     event_dict = reader.read(decoder)
#     return event_dict