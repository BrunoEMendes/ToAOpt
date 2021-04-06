# multithreading
import json
from multiprocessing import Process
import os


# mqtt
from mqqtclient import MQTTClient

# handler
from handler import HandlerServer

def start_server():
    h = HandlerServer('127.0.0.1', 8000)
    h.start_server()


def mqtt_sub():
    mqtt_sub = MQTTClient('192.168.1.9', 1883, topic = 'test')
    mqtt_sub.start()


if __name__ == '__main__':
    p_handler = Process(target=start_server)
    p_mqtt = Process(target=mqtt_sub)

    process = [p_handler, p_mqtt]

    [p.start() for p in process]
    [p.join() for p in process]
    # p_handler.start()
    # p.join()


