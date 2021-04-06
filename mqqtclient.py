import paho.mqtt.client as paho


class MQTTClient():
    def __init__(self, server, port, topic):
        self.server = server
        self.port = port
        self.topic = topic

    def __connect_msg(self, client, userdata, flags, rc):
        print(f'Subscribed: {self.topic} at {self.server}:{self.port}')

    def __message_received(self, client, userdata, msg):
        print(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))    

    def start(self):
        client = paho.Client()
        client.on_connect = self.__connect_msg
        client.on_message = self.__message_received
        client.connect(self.server, self.port)
        client.subscribe(self.topic)
        client.loop_forever()

    
