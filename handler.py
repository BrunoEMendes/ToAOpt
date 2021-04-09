from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from google.protobuf.json_format import Parse
from chirpstack_api.as_pb import integration
from functools import partial

from mqttclient import MQTTClient
from functools import partial



class Handler(BaseHTTPRequestHandler):
    # True -  JSON marshaler
    # False - Protobuf marshaler (binary)
    json = True

    def __init__(self, mqtt_broker_ip, mqtt_broker_port, topic, *args, **kwargs):
        '''
        INIT HANDLER
        '''
        self.mqtt_broker_ip = mqtt_broker_ip
        self.mqtt_broker_port = mqtt_broker_port
        self.topic = topic
        super().__init__(*args, **kwargs)


    def do_POST(self):
        '''
        Queries chirpstack for events
        '''
        self.send_response(200)
        self.end_headers()
        query_args = parse_qs(urlparse(self.path).query)


        content_len = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_len)

        if query_args["event"][0] == "up":
            self.up(body)

        else:
            print("handler for event %s is not implemented" % query_args["event"][0])

    def up(self, body):
        '''
        unmarshals uplinks and sends to mqtt broker
        '''
        up = self.unmarshal(body, integration.UplinkEvent())
        print("Uplink received from: %s with payload: %s" % (up.dev_eui.hex(), up.data.hex()))



        #publish data to mqtt borker
        mqtt = MQTTClient(self.mqtt_broker_ip, self.mqtt_broker_port, self.topic)
        message = {
            "dev_eui"  : up.dev_eui.hex(), 
            "crc"      : str(up.tx_info.lora_modulation_info.code_rate), 
            "sf"       : int(up.tx_info.lora_modulation_info.spreading_factor),
            "rssi"     : int(up.rx_info[0].rssi),
            "channel"  : int(up.rx_info[0].channel),
            "bw"       : int(up.tx_info.lora_modulation_info.bandwidth),
            "f_port"   : int(up.f_port),
            "lora_snr" : str(up.rx_info[0].lora_snr)
        }
        print(message)
        mqtt.start_pub(message)

    def unmarshal(self, body, pl):
        if self.json:
            return Parse(body, pl)

        pl.ParseFromString(body)
        return pl


class HandlerServer():
    '''
    Inits the handler with mqtt broker data and others
    '''
    def __init__(self, server, port, mqtt_broker_server, mqtt_broker_port, topic):
        self.server = server
        self.port = port
        self.mqtt_broker_server = mqtt_broker_server
        self.mqtt_broker_port = mqtt_broker_port    
        self.topic = topic

    def start_server(self):
        handler = partial(Handler, self.mqtt_broker_server, self.mqtt_broker_port, self.topic)
        httpd = HTTPServer(('', self.port), handler)
        httpd.serve_forever()