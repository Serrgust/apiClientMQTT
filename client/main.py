import sys
import paho.mqtt.client as mqtt
import json
import time
from handlers.meters import Meters
import threading

broker_url = "192.168.4.133"
broker_port = 1883

username = 'admin'
password = '1234'
mqtt.Client.connected_flag = False  # create flag in class


def connect_mqtt():
    def on_disconnect(client, userdata, rc):
        global flag_connected
        flag_connected = False
        print("Client Got Disconnected")

    def on_publish(client, userdata, mid):
        print("JSON published")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            global flag_connected
            flag_connected = True
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt.Client("from_api")
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    client.connect(broker_url, broker_port)
    return client


def publish_kwh(client, list_of_kwh):
    for content in list_of_kwh:
        msg = json.dumps(content)
        print(content)
        print(content["Meter"])
        publish_result = client.publish("meter/kwh/" + str(content["Meter"]), msg)
        time.sleep(1)


def publish_kw(client, list_of_kw):
    for content in list_of_kw:
        msg = json.dumps(content)
        print(content)
        print(content["Meter"])
        publish_result = client.publish("meter/" + str(content["Meter"]), msg)
    #    time.sleep(1)


def publish_meter(client, list_of_readings):
    for content in list_of_readings:
        msg = json.dumps(content)
        print(content)
        publish_result = client.publish("meter/" + str(content["Meter"]), msg)
        time.sleep(0.5)


def publish_sites(client, list_of_sites):
    for content in list_of_sites:
        msg = json.dumps(content)
        print(content)
        publish_result = client.publish("meter/sites", msg)


def retrieve_kwh_from_api(client):
    all_meters = Meters().get_every_meter_summary_reading_kwh()
    publish_kwh(client, all_meters)


def retrieve_kw_from_api(client):
    all_meters = Meters().get_every_meter_summary_reading()
    publish_kw(client, all_meters)


def retrieve_meter_from_api(client):
    while 1:
        all_meters = Meters().get_every_meter_summary_reading()
        publish_meter(client, all_meters)
        time.sleep(180)


def retrieve_sites_from_api(client):
    all_sites = Meters().get_all_account_addresses()
    sites = Meters().update_sites(all_sites)
    publish_sites(client, sites)


def publish_temp(client, temp_json):
    for content in temp_json:
        msg = json.dumps(content)
        print(content)
        publish_result = client.publish("sites/temp", msg)
        time.sleep(0.5)


def retrieve_temp_from_api(client):
    while 1:
        temp = Meters().get_temperature_by_zip()
        publish_temp(client, temp)
        time.sleep(360)


def on_meter_date_messages(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    s = msg.payload.decode()
    s = s.replace("\'", "\"")
    a = json.loads(s)
    print(a)


def run():
    a = {"timestamp": 1666275764993, "metrics": [
        {"name": "Request/Data/DB_Request", "timestamp": 1666275763993, "dataType": "String",
         "value": "32011,2022-10-14,2022-10-15,2022-10-16,2022-10-17,2022-10-18,2022-10-19,2022-10-20"}], "seq": 14}
    client = connect_mqtt()
    client.publish("spBv1.0/DB_Request/DDATA/EDGE/Request", str(a))
    client.subscribe("history/kwh")
    #kwh_callback = threading.Thread(target=retrieve_kw_from_api(client), daemon=True)
    #kw_callback = threading.Thread(target=retrieve_kwh_from_api(client), daemon=True)
    retrieve_temp = threading.Thread(target=retrieve_temp_from_api, args=(client,), daemon=True)
    retrieve_meter = threading.Thread(target=retrieve_meter_from_api, args=(client,), daemon=True)
    retrieve_meter.start()
    retrieve_temp.start()
    try:
        while True:
            # client.publish("spBv1.0/DB_Request/DDATA/EDGE/Request", str(a))
            # retrieve_meter.start()
            # retrieve_temp.start()
            client.loop_start()
    except KeyboardInterrupt:
        print("Ending")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
