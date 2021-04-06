from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from google.protobuf.json_format import Parse
from chirpstack_api.as_pb import integration


class Handler(BaseHTTPRequestHandler):
    # True -  JSON marshaler
    # False - Protobuf marshaler (binary)
    json = True

    def do_POST(self):
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
        up = self.unmarshal(body, integration.UplinkEvent())
        print("Uplink received from: %s with payload: %s" % (up.dev_eui.hex(), up.data.hex()))
        
        # add_up(dev_eui  = up.dev_eui, 
        #        crc      = up.tx_info.lora_modulation_info.code_rate, 
        #        sf       = int(up.tx_info.lora_modulation_info.spreading_factor),
        #        rssi     = int(up.rx_info[0].rssi),
        #        channel  = int(up.rx_info[0].channel),
        #        bw       = int(up.tx_info.lora_modulation_info.bandwidth),
        #        f_port   = int(up.f_port),
        #        lora_snr = up.rx_info[0].lora_snr
        #        )

    def unmarshal(self, body, pl):
        if self.json:
            return Parse(body, pl)

        pl.ParseFromString(body)
        return pl


class HandlerServer():
    def __init__(self, server, port):
        self.server = server
        self.port = port

    def start_server(self):
        httpd = HTTPServer(('', self.port), Handler)
        httpd.serve_forever()