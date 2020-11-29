
import json
import os
import ssl
import time

from datetime import datetime

import paho.mqtt.client as mqtt

from pigpio_dht import DHT22

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
# Main python routine.
# This program connect securely to a MQTT server and sends data that looks like
# temperature and humidity reading from a DHT22 sensor in JSON format.
#
if __name__ == '__main__':
    print('MQTT TLS Publisher Test, v1.0')

    # Create MQTT client with client identification.
    client = mqtt.Client("ld57-publisher")

    # Define callback function for connect, disconnect, and log events from MQTT client.
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_log = on_log

    # Define TLS connection properties.
    client.tls_set(ca_certs='etc/trusted-ca.crt', tls_version=ssl.PROTOCOL_TLSv1_2)

    #
    # Set username and password from environment variables.
    #   Windows:
    #     set MQTT_USER=<username>
    #     set MQTT_PWD=<password>
    #    Mac/Linux:
    #      export MQTT_USER=<username>
    #      export MQTT_PWD=<password>
    #    
    client.username_pw_set(os.environ.get('MQTT_USER'), os.environ.get('MQTT_PWD'))

    # Connect to server.
    client.connect(os.environ.get('MQTT_HOST'), int(os.environ.get('MQTT_PORT')))
    
    # Loop until we are connected ... (may loop forever).
    print('Connecting ...')
    while not is_connected:
        client.loop()
        time.sleep(1)

    gpio = 25
    sensor = DHT22(gpio)
    
    try:
        while True:
            # Read DHT22 sensor.
            result = sensor.read()
            if result:
                # Print the result on the console.
                print(result)
                if result.get('valid'):
                    # If result is valid, get temperature and humidity.
                    temperature = result.get('temp_c')
                    humidity = result.get('humidity')

                    # Create payload with timestamp, temperature, and humidity.
                    payload = {}
                    payload['timestamp'] = datetime.utcnow().isoformat(timespec='milliseconds')
                    payload['temperature'] = temperature
                    payload['humidity'] = humidity

                    # Convert payload to JSON.
                    json_payload = json.dumps(payload)

                    # Publish, i.e. send data to server on address iot/device/havreholm-indoor/data
                    print('Publish data to iot/device/laskedalen-office/data ...')
                    client.publish('iot/device/laskedalen-office/data', json_payload)
                    client.loop()
                else:
                    print('Invalid values read from sensor.')
            else:
                print('No value returned from sensor.')

            time.sleep(5)


    except KeyboardInterrupt:
        print('Disconnecting ...')
        client.disconnect()
        client.loop()
        print('Done.')

       
    # Simulate creating a payload with a timestamp, temperature, and humidity reading from a DHT22 sensor.
 

