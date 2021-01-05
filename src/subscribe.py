
import json
import os
import ssl
import sys
import time

from datetime import datetime

import paho.mqtt.client as mqtt

is_connected = False

#
# on_connect()
# Called by the MQTT client to report connection success/error.
#
def on_connect(client, userdata, flags, rc):
    global is_connected

    is_connected = False
    if rc == 0:
        print(f'Connection successful. Userdata={userdata}, flags={flags}.')        
        is_connected = True

        # Subscribe to any data sent to the topic specified as parameter.
        print(f'Subscribe to {topic}')
        client.subscribe(topic, qos=1)

    elif rc == 1:
        print(f'Connection refused. Bad protocol version.')
    elif rc == 2:
        print(f'Connection refused. Invalid client identifier.')
    elif rc == 3:
        print(f'Connection refused. Server unavailable.')
    elif rc == 4:
        print(f'Connection refused. Invalid username/password.')
    elif rc == 5:
        print(f'Connection refused. Unauthorized.')
    else:
        print(f'Connection refused. Unknown reason.')

#
# on_disconnect()
# Called by MQTT client to report disconnection from server.
#
def on_disconnect(client, userdata, rc):
    global is_connected

    is_connected = False
    if rc == 0:
        print('Disconnected on client request.')
    else:
        print('Unexpected disconnect from server.')
    
#
# on_log()
# Called by MQTT client to report status of the client.
#
def on_log(client, userdata, level, buf):
    print(f'log: {level} - {buf}')

#
# on_message()
# Called by the MQTT client whenever a message is received.
#
def on_message(client, userdata, message):
    print(f'Message received: topic={message.topic}, payload={message.payload}.')

#
# Main python routine.
# This program connect securely to a MQTT server and subscribes to a topic.
#
if __name__ == '__main__':
    print('MQTT TLS Subscriber Test, v1.0')

    # Get topic to subscribe to.
    if len(sys.argv) == 3:
        client_id = sys.argv[1]
        topic = sys.argv[2]
    else:
        print(f'Usage: python subscribe.py [client id] [topic]')
        sys.exit(1)

    # Create MQTT client with client identification.
    print(f'Create client {client_id} ...')
    client = mqtt.Client(client_id=client_id, clean_session=False, userdata="spunk")

    # Define callback function for connect, disconnect, and log events from MQTT client.
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_log = on_log
    client.on_message = on_message

    # Define TLS connection properties.
    client.tls_set(ca_certs='etc/trusted-ca.crt', tls_version=ssl.PROTOCOL_TLSv1_2)

    #
    # Set username and password from environment variables.
    #   MQTT_USER
    #   MQTT_PWD
    # 
    client.username_pw_set(os.environ.get('MQTT_USER'), os.environ.get('MQTT_PWD'))

    # Connect to server using hostname and port from environment variables.
    #   MQTT_HOST
    #   MQTT_PORT
    # 
    client.connect(os.environ.get('MQTT_HOST'), int(os.environ.get('MQTT_PORT')))
    
    # Loop until we are connected ... (may loop forever).
    print('Connecting ...')
    while not is_connected:
        client.loop()
        time.sleep(1)

    

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print('Disconnecting ...')
        client.disconnect()
        client.loop()
        print('Done.')
