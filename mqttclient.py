import paho.mqtt.client as paho
import requests

import json
import base64

from datetime import datetime

class MQTTClient():
    def __init__(self, mqtt_broker_ip, mqtt_broker_port, topic):
        self.mqtt_broker_ip = mqtt_broker_ip
        self.mqtt_broker_port = mqtt_broker_port
        self.topic = topic


    # private functions
    def __connect_msg(self, client, userdata, flags, rc):
        '''
        message when mqtt connects to broker successfully
        '''
        print(f'Subscribed: {self.topic} at {self.mqtt_broker_ip}:{self.mqtt_broker_port}')

    def __message_received(self, client, userdata, msg):
        '''
        callback function:
            -   this function is called whenever it successfully reads a message
                from the broker.
            -   will convert the example payload received to a POST message payload 
                and send it to chirpstack
   
        Example payload received
        {
                "dev_eui":"fdaffdfaadbfadfa", 
                "fPort":1
                "crc": "4/5",
                "bw" : 125,
                "sf" :  11,
                "snr": "21"
        }

        Chirpstack Payload
                msg = {
                "deviceQueueItem": {
                    "confirmed": True,
                    "data": string,
                    "devEUI": dev_eui,
                    "fCnt": 0,
                    "fPort": port
                }
        }
        '''
        print(f'Message read from {msg.topic} at {datetime.now()}')

        object_json = self.__decode(msg.payload)
 
        print('--------------------------------------------------')
        # print(self.__encode(object_json['opt']))
        dev_eui = object_json['dev_eui']
        
        data = {
            "crc": object_json['crc'],
            "bw" : object_json['bw'],
            "sf" : object_json['sf'],
            "snr": object_json['snr']
        }

        msg = {
                "deviceQueueItem": {
                    "confirmed": True,
                    "data": self.__encode(data).decode('ascii'),
                    "devEUI": dev_eui,
                    "fCnt": 0,
                    "fPort": object_json['fPort']
                }
        }
        print(f'Post message sent to Chirpstack API at {datetime.now()}')
        print('Message format: ')
        print(msg)

        api_url = f'{self.chirpstack_api}/api/devices/{dev_eui}/queue'
        req = requests.post(api_url, headers= self.__headers, data=json.dumps(msg))
        print(req)   # 200 - ok / 404 - error / etc 

    def __connect(self):
        '''
        creates a new MQTT CLIENT
        '''
        client = paho.Client()
        client.on_connect = self.__connect_msg

        client.on_message = self.__message_received
        client.on_publish = self.__on_publish

        client.connect(self.mqtt_broker_ip, self.mqtt_broker_port)
        return client

    def __on_publish(self, client, userdata, result):
        '''
        Callback function that is called whenever the data is published
        '''
        print(f'Packet published to {self.topic} in {self.mqtt_broker_ip}:{self.mqtt_broker_port} at {datetime.now()}')

    def __decode(self, message):
        '''
        decodes Base64 stuff and converts to json object
        '''
        decoded = base64.b64decode(message).decode('ascii')
        return json.loads(decoded)

    def __encode(self, message):
        '''
        convers json object to base64
        '''
        return base64.b64encode(json.dumps(message).encode('ascii'))


    #public functions

    def start_pub(self, message):
        '''
        publishes a message
        '''
        client = self.__connect()
        ret = client.publish(self.topic, self.__encode(message))


    def start_sub(self, chirpstack_api, chirpstack_api_key):
        '''
        creates indefinitive time subscriber client to MQTT broker
        '''

        self.chirpstack_api = chirpstack_api
        self.__headers = {'Accept': 'application/json', 'Grpc-Metadata-Authorization': f'Bearer {chirpstack_api_key}'}
        
        client = self.__connect()
        client.subscribe(self.topic)
        client.loop_forever()

    
