from random import choice
import sys
# import argparse
import logging
import configparser
import json
import requests
from kafkaConsumer import *
from kafkaProducer import kafkaProducer, kafkaAvroProducer
import urllib3

urllib3.disable_warnings()


configParser = configparser.RawConfigParser()   
configFilePath = r'config.ini'
configParser.read(configFilePath)

# argParser = argparse.ArgumentParser()
# argParser.add_argument("-f", "--function", help="Function to Call")
# argParser.add_argument("-c", "--commit", help="Commit value")


logLevel=configParser.get('logging', 'logging.level')
logging.basicConfig()
logging.getLogger().setLevel(logLevel)
running = True

topic=configParser.get('confluent', 'write.topic')
sasTokenUrl=configParser.get('sas', 'token.url')
sasExecuteUrl=configParser.get('sas', 'execute.url')
clientId=configParser.get('sas', 'client.id')
clientSecret=configParser.get('sas', 'client.secret')
resultTopic=configParser.get('confluent', 'result.topic')

# access_token=return_access_token()

def get_new_token():
    auth_server_url = sasTokenUrl
    client_id = clientId
    client_secret = clientSecret

    token_req_payload = {'grant_type': 'client_credentials'}

    token_response = requests.post(auth_server_url,
    data=token_req_payload, verify=False, allow_redirects=False,
    auth=(client_id, client_secret))
                
    if token_response.status_code !=200:
        logging.warn("Failed to obtain token from the OAuth 2.0 server")
        # sys.exit(1)

    print("Successfuly obtained a new token")
    tokens = json.loads(token_response.text)
    access_token=tokens['access_token']
    return access_token

def postDataToSas(message,key):
    logging.info('Posting data to Sass')
    access_token=get_new_token()
    # tmp = "{\"version\":1,\"inputs\":[{\"name\":\"AVG_SAL_L3M_\",\"value\":22891.50607},{\"name\":\"Bureau_Score_\",\"value\":646.9684456},{\"name\":\"card_type_\",\"value\":\"Platinum\"},{\"name\":\"Credit_Limit_\",\"value\":17395.56729},{\"name\":\"Current_Balance_to_Credit_Limit_\",\"value\":88.172723954},{\"name\":\"Delinquent_Indicator_\",\"value\":\"NonRegular\"},{\"name\":\"Number_Of_Credit_Cards_\",\"value\":9},{\"name\":\"salary_\",\"value\":23021.43755},{\"name\":\"Tot_Credit_Card_Bal_L3M_\",\"value\":-3994},{\"name\":\"Tot_Credit_Card_Bal_L6M_\",\"value\":-4088}]}"
    api_call_headers = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/json'}
    print("Message : " + json.dumps(message))
    api_call_response = requests.post(sasExecuteUrl, json=message, headers=api_call_headers, verify=False)
    print(str(api_call_response.json()))
    
    if	api_call_response.status_code == 401:
        access_token = get_new_token()
    else:
        if api_call_response.json().get('version') == 2:
            # logging.info(api_call_response.text)
            logging.info('Successfull response from SAS')
            kafkaProducer(resultTopic,key,json.dumps(api_call_response.json()))
        elif api_call_response.json().get('errorCode') == 0:
            logging.error(api_call_response.text)
        else:
            logging.info(api_call_response.text)

def process_msg(key,message):
    logging.info("Key : " + str(key))
    logging.info("Value : " + str(message))
    logging.info("topic {}".format(topic))
    kafkaProducer(topic,key,json.dumps(message))
    postDataToSas(message,key)
    # kafkaAvroProducer(topic,key,message)
    


if __name__ == "__main__":
    successfullRun = False
    kafkaConsumer(True)
    if successfullRun == False:
        logging.error("Run failed! Function {} couldn't executed".format(function))
        sys.exit()

