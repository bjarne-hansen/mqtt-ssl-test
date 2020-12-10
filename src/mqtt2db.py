
from configparser import ConfigParser
from datetime import datetime
import json
import os
import paho.mqtt.client as mqtt
from pathlib import Path
import re
import sys
import ssl
import time

import psycopg2
from psycopg2 import InterfaceError, DatabaseError, DataError, OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError
from psycopg2.extras import Json
from psycopg2.pool import ThreadedConnectionPool

_pool = None
_mqtt_connected = False

_topic_expression = re.compile(r'iot/(.*)/(.*)/data')

def connect_database(pool_min, pool_max, database, user, password, host, port):
    global _pool    
    _pool = ThreadedConnectionPool(pool_min, pool_max, database=database, user=user, password=password, host=host, port=port)


#
# on_connect()
# Called by the MQTT client to report connection success/error.
#
def on_connect(client, userdata, flags, rc):
    global _mqtt_connected

    _mqtt_connected = False
    if rc == 0:
        print(f'Connection successful. Userdata={userdata}, flags={flags}.')        
        _mqtt_connected = True
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

    match = _topic_expression.match(message.topic)
    if match:
        try:
            payload = json.loads(message.payload)
            if 'timestamp' in payload: 
                timestamp = datetime.fromisoformat(payload['timestamp'])
            else:
                timestamp = datetime.utcnow()
        except:
            timestamp = None
            payload = None

        if payload:                
            connection = _pool.getconn()
            with connection as connection:
                with connection.cursor() as cursor:
                    username = match.group(1)
                    device_name = match.group(2)
                    payload = str(message.payload, 'utf-8')
                    print(f'Sensor data received for account={username}, device={device_name}, payload={payload}.')
                    cursor.execute('INSERT INTO "data" ("timestamp", "account_id", "device_id", "values") SELECT %s, a.id, d.id, %s FROM account a, device d WHERE a.username=%s AND d.name=%s RETURNING id;', (timestamp, payload, username, device_name))
                    row = cursor.fetchone()
                    if row:
                        print(f'id={row[0]}')
                    else:
                        print(f'No data inserted in database.')
                    cursor.close()
            _pool.putconn(connection)
        else:
            print('Invalid payload.')
            


def create_mqtt_client(client_id):
    # Create MQTT client with client identification.
    print(f'Create client {client_id} ...')
    client = mqtt.Client(client_id=client_id)

    # Define callback function for connect, disconnect, and log events from MQTT client.
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_log = on_log
    client.on_message = on_message

    # Define TLS connection properties.
    client.tls_set(ca_certs='etc/trusted-ca.crt', tls_version=ssl.PROTOCOL_TLSv1_2)

    
#
# Main python routine.
# 
# Subscribe to topic iot/+/+/data and take JSON published on matching topics and store
# then in a PostgreSQL database with the TimescaleDB extension.
#
if __name__ == '__main__':
    print('MQTT-2-DB Subscriber, v1.0')

    # Determine available configuration files.
    config = ConfigParser()
    config_files = []
    
    if Path('.mqtt2db.conf').exists:
        config_files.append('.mqtt2db.conf')

    if len(sys.argv) == 2:
        if Path(sys.argv[1]).exists:
            config_files.append(sys.argv[1])
        else:
            print(f'Configuration file {sys.argv[1]} does not exist.')
            sys.exit(1)

    # Read configuration.
    config = ConfigParser()
    config.read(config_files)

    mqtt_client_id = config.get('mqtt', 'client_id')
    mqtt_host = config.get('mqtt', 'host')
    mqtt_port = config.getint('mqtt', 'port', 8883)
    mqtt_user = config.get('mqtt', 'user')
    mqtt_password = config.get('mqtt', 'password')
    mqtt_topic = config.get('mqtt', 'topic')

    min_pool = config.getint('database', 'min_pool', 1)
    max_pool = config.getint('database', 'max_pool', 5)
    host = config.get('mqtt', 'host')
    port = config.getint('mqtt', 'port', 5432)
    database = config.get('database', 'database')
    user = config.get('database', 'user')
    password = config.get('database', 'password')    
    
    # Connect to database.
    connect_database(min_pool, max_pool, database, user, password, host, port)

    # Create MQTT client.
    client = create_mqtt_client(mqtt_client_id)
    client.username_pw_set(mqtt_user, mqtt_password)
    client.connect(mqtt_host, int(os.environ.get('MQTT_PORT')))
    
    # Loop until we are connected ... (may loop forever).
    print('Connecting ...')
    while not _mqtt_connected:
        client.loop()
        time.sleep(1)

    # Subscribe to any data sent to the topic specified as parameter.
    print(f'Subscribe to {mqtt_topic}')
    client.subscribe(mqtt_topic)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print('Disconnecting ...')
        client.disconnect()
        client.loop()
        print('Done.')
