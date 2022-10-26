import urllib.request
import urllib.error
import urllib.parse
import json

from client.config.api_key import API_KEY
from client.config.api_key import WEATHER_KEY
from client.model.main_dao import MainDAO


class MetersDAO(MainDAO):

    def __init__(self):
        MainDAO.__init__(self)

    @staticmethod
    def get_last_meters_reading_summary():
        api_request = 'https://api.ekmpush.com/readMeter?key=' + API_KEY + '&cnt=1&format=json&fields=kWh_Tot~RMS_Watts_Tot&timezone=America~Puerto_Rico'
        response = urllib.request.urlopen(api_request)
        response = response.read()
        json_object = json.loads(response.decode())
        return json_object

    def get_all_account_addresses(self):
        api_request = 'https://api.ekmpush.com/account/api/account?key=' + API_KEY
        response = urllib.request.urlopen(api_request)
        response = response.read()
        json_object = json.loads(response.decode())
        return json_object

    def get_gateway(self, gateway_id):
        gateway = str(gateway_id)
        api_request = 'https://api.ekmpush.com/account/api/gateway?key=' + API_KEY + "&id=" + gateway
        response = urllib.request.urlopen(api_request)
        response = response.read()
        json_object = json.loads(response.decode())
        return json_object

    def get_gateway_name(self, gateway_id):
        gateway = str(gateway_id)
        api_request = 'https://api.ekmpush.com/account/api/gateway?key=' + API_KEY + "&id=" + gateway
        response = urllib.request.urlopen(api_request)
        response = response.read()
        json_object = json.loads(response.decode())
        account_addresses = json_object['gateways']
        name = account_addresses[0]['name']
        return name

    def get_gateway_meters(self, gateway_id):
        gateway = str(gateway_id)
        api_request = 'https://api.ekmpush.com/account/api/gateway?key=' + API_KEY + "&id=" + gateway
        response = urllib.request.urlopen(api_request)
        response = response.read()
        json_object = json.loads(response.decode())
        account_addresses = json_object['gateways']
        result_helper = account_addresses[0]['meters']
        return result_helper

    def get_temp_by_zip(self, zipcode):
        api_request = 'https://api.openweathermap.org/data/2.5/weather?units=imperial&zip=' + zipcode + ',PR&appid=' + WEATHER_KEY
        response = urllib.request.urlopen(api_request)
        response = response.read()
        json_object = json.loads(response.decode())
        return json_object

