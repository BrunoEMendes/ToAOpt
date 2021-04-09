#general imports
import json
import sys

# multithreading
from multiprocessing import Process

# mqtt
from mqttclient import MQTTClient

# handler
from handler import HandlerServer

# -------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------ generic functions ------------------------------------------------------ 
# -------------------------------------------------------------------------------------------------------------------------------

def read_data(filename):
    '''
    checks if its a json file and returns the data in it as a json object
    '''
    try:
        assert filename[-5:] == '.json'
    except:
        raise Exception('Invalid file type')
    file_data = open(filename, 'r')
    return json.loads(file_data.read())

# --------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------ multithreading functions ------------------------------------------------------ 
# --------------------------------------------------------------------------------------------------------------------------------------


def start_server(mqtt_broker, mqtt_port, topic, ip = '', port = 8000):
    '''
    starts the handler server:
        - filters the data received from Chirpstack API and published on MQTT broker
    '''
    h = HandlerServer(ip, port, mqtt_broker, mqtt_port, topic)
    h.start_server()


def post_downlink(chirpstack_api, chirpstack_api_key, server, port = '1883', topic = ''):
    '''
    starts the mqtt client subscriber:
        - converts the data received from MQTT broker and posts it to Chirpstack Broker
    '''
    mqtt_sub = MQTTClient(server, port, topic)
    mqtt_sub.start_sub(chirpstack_api, chirpstack_api_key)



# ------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------ MAIN ------------------------------------------------------ 
# ------------------------------------------------------------------------------------------------------------------
def main():
    # checks if the config file is somewhat valid (doesnt work that well)
    # also reads the data from the file
    try:
        file_data = read_data(sys.argv[1])
    except:
        raise Exception('No valid file as argument 1')


    # init handler
    p_handler = Process(target=start_server, args=( file_data['mqtt_broker']['ip'], 
                                                    file_data['mqtt_broker']['port'],
                                                    file_data['server']['topic'],
                                                    file_data['server']['ip'],
                                                    file_data['server']['port']))

    # init MQTT subscriber
    p_mqtt = Process(target=post_downlink, args=(   file_data['chirpstack_api']['server'],
                                                    file_data['chirpstack_api']['key'],
                                                    file_data['mqtt_broker']['ip'], 
                                                    file_data['mqtt_broker']['port'],
                                                    file_data['mqtt_broker']['topic']))

    # process list
    process = [p_handler, p_mqtt]

    # init multithreading
    [p.start() for p in process]
    [p.join() for p in process]


if __name__ == '__main__':
    main()



