from pigpio_dht import DHT22

if __name__ == '__main__':
    gpio = 25
    sensor = DHT22(gpio)
    result = sensor.read()
    print(result)
