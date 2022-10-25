from client.model.meters_dao import MetersDAO


def build_meter_dict(self, readings):
    new_dict = {
        'Meter': int(readings['Meter']),
        'Time': readings['ReadData'][0]['Time'],
        'Date': readings['ReadData'][0]['Date'],
        'Time_Stamp_UTC_ms': readings['ReadData'][0]['Time_Stamp_UTC_ms'],
        'RMS_Watts_Tot': int(),
        'kWh_Tot': float(),
        'Good': int(readings['ReadData'][0]['Good'])
    }
    if 'kWh_Tot' in readings['ReadData'][0]:
        new_dict.update({'kWh_Tot': float(readings['ReadData'][0]['kWh_Tot'])}),
    if 'RMS_Watts_Tot' in readings['ReadData'][0]:
        new_dict.update({'RMS_Watts_Tot': int(readings['ReadData'][0]['RMS_Watts_Tot'])}),
    return new_dict


def build_kwh_dict(self, readings):
    new_dict = {
        'Meter': int(readings['Meter']),
        'Time': readings['ReadData'][0]['Time'],
        'Date': readings['ReadData'][0]['Date'],
        'Time_Stamp_UTC_ms': readings['ReadData'][0]['Time_Stamp_UTC_ms'],
        'kWh_Tot': float(),
        'Good': int(readings['ReadData'][0]['Good'])
    }
    if 'kWh_Tot' in readings['ReadData'][0]:
        new_dict.update({'kWh_Tot': float(readings['ReadData'][0]['kWh_Tot'])}),
    return new_dict


def build_site_dict(self, name, meter, mac_address):
    new_dict = {
        'Name': name,
        'Meter': meter,
        'Mac_Address': mac_address
    }
    return new_dict


def build_temp_dict(self, readings):
    return {
        'Temp': readings['main']['temp'],
        'Humidity': readings['main']['humidity'],
        'City': readings['name'],
        'Country': readings['sys']['country'],
        'Weather Condition': readings['weather'][0]['main'] + ' -' + readings['weather'][0]['description'],
    }


zipcodes = [
    "00988",  # Carolina
    "00960",  # Bayamon
    "00969",  # Guaynabo
    "00926"  # San Juan
]


class Meters:

    def get_every_meter_summary_reading_kwh(self):
        dao = MetersDAO()
        reading = dao.get_last_meters_reading_summary()
        meters = reading['readMeter']['ReadSet']
        result_list = []
        for meter in meters:
            new_dict = build_kwh_dict(self, meter)
            result_list.append(new_dict)
        return result_list

    def get_every_meter_summary_reading(self):
        dao = MetersDAO()
        reading = dao.get_last_meters_reading_summary()
        meters = reading['readMeter']['ReadSet']
        result_list = []
        for meter in meters:
            new_dict = build_meter_dict(self, meter)
            result_list.append(new_dict)
        return result_list

    def get_all_account_addresses(self):
        dao = MetersDAO()
        meters = dao.get_all_account_addresses()
        account_addresses = meters['gateways']
        result_list = []
        # for row in meters:
        #     obj = self.build_map_dict_meters(row)
        #     result_list.append(obj)
        return account_addresses

    def get_gateway(self, gateway_id):
        dao = MetersDAO()
        info = dao.get_gateway(gateway_id)
        account_addresses = info['gateways']
        result_list = []
        # for row in meters:
        #     obj = self.build_map_dict_meters(row)
        #     result_list.append(obj)
        return account_addresses

    def get_gateway_name(self, gateway_id):
        dao = MetersDAO()
        info = dao.get_gateway_name(gateway_id)
        return info

    @staticmethod
    def get_gateway_meters(gateway_id):
        dao = MetersDAO()
        info = dao.get_gateway_meters(gateway_id)
        return info

    def update_sites(self, received_json):
        result_list = []
        for x in received_json:
            gateway_name = str(self.get_gateway_name(x['mac_address']))
            gateway_meters = (self.get_gateway_meters(x['mac_address']))
            mac_address = str(x['mac_address'])
            for y in gateway_meters:
                new_dict = build_site_dict(self, gateway_name, str((int(y["address"]))), mac_address)
                result_list.append(new_dict)
        return result_list

    def get_temperature_by_zip(self):
        dao = MetersDAO()
        result_list = []
        for a in zipcodes:
            readings = dao.get_temp_by_zip(a)
            new_dict = build_temp_dict(self, readings)
            result_list.append(new_dict)
        return result_list
