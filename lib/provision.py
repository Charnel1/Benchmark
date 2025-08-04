# create table
from lib.db import _query_data, _fetch_data

import pandas as pd
import requests


# Table Provision Carrier
# Table Provision Partner
def create_ProvsionCarrierTable():
    _query_data("""CREATE TABLE mysql.ProvisionCarrier (
            id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
            carrier varchar(255)    NOT NULL, 
            Laufzeit varchar(20)    NOT NULL, 
            MRR FLOAT               NOT NULL, 
            Seat FLOAT              NOT NULL, 
            Aktivierung FLOAT       NOT NULL, 
            Push FLOAT              NOT NULL,  
            Airtime FLOAT           NOT NULL
            );  
    """)


def create_ProvsionPartnerTable():
    _query_data("""
            CREATE TABLE mysql.ProvisionPartner(
            id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
            PartnerKurz varchar(255)    NOT NULL, 
            PartnerLang varchar(255)    NOT NULL, 
            carrier varchar(255)    NOT NULL,
            Laufzeit varchar(20)    NOT NULL, 
            MRR FLOAT               NOT NULL, 
            Seat FLOAT              NOT NULL, 
            Aktivierung FLOAT       NOT NULL, 
            Push FLOAT              NOT NULL,  
            Airtime FLOAT           NOT NULL
            );  
    """)


def create_ProvsionServiceTable():
    _query_data("""
            CREATE TABLE mysql.ProvisionService(
            id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
            PartnerKurz varchar(255)    NOT NULL, 
            PartnerLang varchar(255)    NOT NULL, 
            Abschluss FLOAT             NOT NULL, 
            Airtime FLOAT               NOT NULL
            );  
    """)


def add_Row_Provsion_Carrier(_carrier, _Laufzeit, _MRR, _Seat, _Aktivierung, _Push, _Airtime):
    _query_data(f"""INSERT INTO mysql.ProvisionCarrier (
                            carrier,Laufzeit, MRR, Seat, Aktivierung, Push, Airtime) 
                            VALUES('{_carrier}', '{_Laufzeit}', {_MRR}, {_Seat}, {_Aktivierung}, {_Push}, {_Airtime});""")


def add_Row_Provsion_Service(_PartnerKurz, _PartnerLang, _Abschluss, _Airtime):
    _query_data(f"""INSERT INTO mysql.ProvisionService (
                            PartnerKurz,
                            PartnerLang,
                            Abschluss,
                            Airtime
                            ) 
                            VALUES('{_PartnerKurz}', '{_PartnerLang}', {_Abschluss}, {_Airtime});""")


def add_Row_Provsion_Partner(_PartnerKurz, _PartnerLang, _carrier, _Laufzeit, _MRR, _Seat, _Aktivierung, _Push, _Airtime):
    _query_data(f"""INSERT INTO mysql.ProvisionPartner (
                            PartnerKurz, PartnerLang, carrier, Laufzeit, MRR, Seat, Aktivierung, Push, Airtime) 
                            VALUES( '{_PartnerKurz}', '{_PartnerLang}', '{_carrier}', '{_Laufzeit}', {_MRR}, {_Seat}, {_Aktivierung}, {_Push}, {_Airtime});""")


def add_new_Provsion_Carrier(_Carrier):
    add_Row_Provsion_Carrier(_Carrier, "24 Monate", 0.2, 0.2, 0.2, 0.2, 0.2)
    add_Row_Provsion_Carrier(_Carrier, "36 Monate", 0.2, 0.2, 0.2, 0.2, 0.2)
    add_Row_Provsion_Carrier(_Carrier, "48 Monate", 0.2, 0.2, 0.2, 0.2, 0.2)
    add_Row_Provsion_Carrier(_Carrier, "60 Monate", 0.2, 0.2, 0.2, 0.2, 0.2)


def add_AllExisting_Provsion_Partner():
    data_partner = _fetch_data(
        "SELECT firmen_tag, Firmenname FROM mysql.Partner_details WHERE NOT firmen_tag = '7Werk'")

    data_carrier = _fetch_data(
        "SELECT DISTINCT carrier FROM mysql.ProvisionCarrier")

    for partner in range(len(data_partner)):
        for carrier in range(len(data_carrier)):
            add_Row_Provsion_Partner(
                str(data_partner[partner].get('firmen_tag')),
                str(data_partner[partner].get('Firmenname')),
                str(data_carrier[carrier].get('carrier')),
                "24 Monate", 0.2, 0.2, 0.2, 0.2, 0.2)

            add_Row_Provsion_Partner(
                str(data_partner[partner].get('firmen_tag')),
                str(data_partner[partner].get('Firmenname')),
                str(data_carrier[carrier].get('carrier')),
                "36 Monate", 0.2, 0.2, 0.2, 0.2, 0.2)

            add_Row_Provsion_Partner(
                str(data_partner[partner].get('firmen_tag')),
                str(data_partner[partner].get('Firmenname')),
                str(data_carrier[carrier].get('carrier')),
                "48 Monate", 0.2, 0.2, 0.2, 0.2, 0.2)

            add_Row_Provsion_Partner(
                str(data_partner[partner].get('firmen_tag')),
                str(data_partner[partner].get('Firmenname')),
                str(data_carrier[carrier].get('carrier')),
                "60 Monate", 0.2, 0.2, 0.2, 0.2, 0.2)


def search_Provsion_Carrier(searchField: list, searchValue: dict):
    '''
    Alle möglichen such Felder:
    carrier Laufzeit MRR Seat Aktivierung Push Airtime

    bsp.
    searchField = ['carrier', 'Laufzeit']
    searchValue = {'carrier': ["WTG"], 'Laufzeit': ["24 Monate"]}
    or 
    searchValue = {'carrier': ["WTG"], 'Laufzeit': ["24 Monate", "48 Monate"]} 
    '''
    searchQuerry = "SELECT carrier,   Laufzeit,  MRR,  Seat,  Aktivierung,  Push,  Airtime FROM mysql.ProvisionCarrier WHERE "

    for element in searchField:
        match element:
            case 'carrier':
                if len(searchValue.get('carrier')) == 1:
                    searchQuerry += "carrier = " + "'" + \
                        searchValue.get('carrier')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('carrier'):
                        searchQuerry += "carrier = " + "'" + items + "'" + " OR  "
            case 'Laufzeit':
                if len(searchValue.get('Laufzeit')) == 1:
                    searchQuerry += "Laufzeit = " + "'" + \
                        searchValue.get('Laufzeit')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('Laufzeit'):
                        searchQuerry += "Laufzeit = " + "'" + items + "'" + " OR  "
            case 'MRR':
                if len(searchValue.get('MRR')) == 1:
                    searchQuerry += "MRR = " + "'" + \
                        searchValue.get('MRR')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('MRR'):
                        searchQuerry += "MRR = " + "'" + items + "'" + " OR  "
            case 'Seat':
                if len(searchValue.get('Seat')) == 1:
                    searchQuerry += "Seat = " + "'" + \
                        searchValue.get('Seat')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('Seat'):
                        searchQuerry += "Seat = " + "'" + items + "'" + " OR  "
            case 'Aktivierung':
                if len(searchValue.get('Aktivierung')) == 1:
                    searchQuerry += "Aktivierung = " + "'" + \
                        searchValue.get('Aktivierung')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('Aktivierung'):
                        searchQuerry += "Aktivierung = " + "'" + items + "'" + " OR  "
            case 'Push':
                if len(searchValue.get('Push')) == 1:
                    searchQuerry += "Push = " + "'" + \
                        searchValue.get('Push')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('Push'):
                        searchQuerry += "Push = " + "'" + items + "'" + " OR  "
            case 'Airtime':
                if len(searchValue.get('Airtime')) == 1:
                    searchQuerry += "Airtime = " + "'" + \
                        searchValue.get('Airtime')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('Airtime'):
                        searchQuerry += "Airtime = " + "'" + items + "'" + " OR  "

    return pd.DataFrame(_fetch_data(searchQuerry[:-4]))


def search_Provsion_Partner(searchField: list, searchValue: dict):
    '''
    Alle möglichen such Felder: PartnerKurz PartnerLang carrier Laufzeit MRR Seat Aktivierung Push Airtime

    bsp.
    searchField = ['carrier', 'Laufzeit']
    searchValue = {'carrier': "WTG", 'Laufzeit': "24 Monate"}
    '''
    searchQuerry = "SELECT id, PartnerLang AS Partnername, carrier,  Laufzeit,  MRR,  Seat,  Aktivierung,  Push,  Airtime FROM mysql.ProvisionPartner WHERE "

    for element in searchField:
        match element:
            case 'Partnername':
                if len(searchValue.get('Partnername')) == 1:
                    searchQuerry += "PartnerLang = " + "'" + \
                        searchValue.get('Partnername')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('Partnername'):
                        searchQuerry += "PartnerLang = " + "'" + items + "'" + " OR  "

            case 'carrier':
                if len(searchValue.get('carrier')) == 1:
                    searchQuerry += "carrier = " + "'" + \
                        searchValue.get('carrier')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('carrier'):
                        searchQuerry += "carrier = " + "'" + items + "'" + " OR  "

            case 'Laufzeit':

                if len(searchValue.get('Laufzeit')) == 1:
                    searchQuerry += "Laufzeit = " + "'" + \
                        searchValue.get('Laufzeit')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('Laufzeit'):
                        searchQuerry += "Laufzeit = " + "'" + items + "'" + " OR  "

    return pd.DataFrame(_fetch_data(searchQuerry[:-4]))


def search_Provsion_Service(searchField: list, searchValue: dict):
    '''
    Alle möglichen such Felder: PartnerKurz PartnerLang carrier Laufzeit MRR Seat Aktivierung Push Airtime

    bsp.
    searchField = ['Partner Name']
    '''
    searchQuerry = "SELECT id, PartnerLang AS Partnername, Abschluss, Airtime FROM mysql.ProvisionService WHERE "

    for element in searchField:
        match element:
            case 'Partnername':
                if len(searchValue.get('Partnername')) == 1:
                    searchQuerry += "PartnerLang = " + "'" + \
                        searchValue.get('Partnername')[0] + "'" + " AND "
                else:
                    for items in searchValue.get('Partnername'):
                        searchQuerry += "PartnerLang = " + "'" + items + "'" + " OR  "

    return pd.DataFrame(_fetch_data(searchQuerry[:-4]))


def get_Provsion_ColumnsPartner():
    return_list = []
    data = _fetch_data(
        "SELECT PartnerKurz, PartnerLang, carrier,  Laufzeit,  MRR,  Seat,  Aktivierung,  Push,  Airtime FROM mysql.ProvisionPartner LIMIT 1")

    for keys, values in data[0].items():
        return_list.append(keys)
    return return_list, data


def get_Provsion_ColumnsCarrier():
    return_list = []
    data = _fetch_data(
        "SELECT carrier, Laufzeit, MRR, Seat, Aktivierung, Push, Airtime FROM mysql.ProvisionCarrier LIMIT 1")
    for keys, values in data[0].items():
        return_list.append(keys)
    return return_list


def get_Provsion_ListOfCarrier():
    return_list = []
    data = _fetch_data(
        "SELECT DISTINCT carrier FROM mysql.ProvisionCarrier")
    for entry in range(len(data)):
        return_list.append(data[entry].get('carrier'))
    return return_list


def get_Provsion_ListOfPartner():
    return_list = []
    data = _fetch_data(
        "SELECT DISTINCT PartnerLang AS Name FROM mysql.ProvisionPartner")
    for entry in range(len(data)):
        return_list.append(data[entry].get('Name'))
    return return_list


def get_Provsion_ListOfCarrierInPartner():
    return_list = []
    data = _fetch_data(
        "SELECT DISTINCT carrier FROM mysql.ProvisionPartner")
    for entry in range(len(data)):
        return_list.append(data[entry].get('carrier'))
    return return_list


def get_Provsion_DataCarrier():
    data = pd.DataFrame(_fetch_data(
        "SELECT * FROM mysql.ProvisionCarrier"))
    return data


def get_Provsion_DataService():
    data = pd.DataFrame(_fetch_data(
        "SELECT * FROM mysql.ProvisionService"))
    return data


def get_Provsion_DataPartner():
    data = pd.DataFrame(_fetch_data(
        "SELECT id, PartnerLang AS Partnername, carrier, Laufzeit, MRR, Seat, Aktivierung, Push, Airtime FROM mysql.ProvisionPartner"))
    return data


def Edit_Provsion_RowCarrier(_id, _carrier, _Laufzeit, _Seat, _mrr, _Aktivierung, _Push, _Airtime):
    _query_data(
        f"UPDATE mysql.ProvisionCarrier SET carrier='{_carrier}',  Laufzeit='{_Laufzeit}', MRR={_mrr}, Seat={_Seat}, Aktivierung = {_Aktivierung}, Push = {_Push}, Airtime = {_Airtime} WHERE id = {_id}")


def Edit_Provsion_RowPartner(_id, _carrier, _Laufzeit, _Seat, _mrr, _Aktivierung, _Push, _Airtime):
    _query_data(
        f"UPDATE mysql.ProvisionPartner SET carrier='{_carrier}',  Laufzeit='{_Laufzeit}', MRR={_mrr}, Seat={_Seat}, Aktivierung = {_Aktivierung}, Push = {_Push}, Airtime = {_Airtime} WHERE id = {_id}")


def Edit_Provsion_RowService(_id, _Abschluss, _Airtime):
    _query_data(
        f"UPDATE mysql.ProvisionService SET Abschluss = {_Abschluss}, Airtime = {_Airtime} WHERE id = {_id}")


def Send_Provsion_RowCarrierZapier(_id, _carrier, _Laufzeit, _Seat, _mrr, _Aktivierung, _Push, _Airtime):
    _url = 'https://hooks.zapier.com/hooks/catch/16731218/uom04gm/'
    _data = {
        'id': _id,
        "carrier": _carrier,
        "Laufzeit": _Laufzeit,
        "Seat": _Seat,
        "mrr": _mrr,
        "Aktivierung": _Aktivierung,
        "Push": _Push,
        "Airtime": _Airtime
    }
    requests.post(url=_url, data=_data)


def Send_Provsion_RowPartnerrZapier(_id, _carrier, _Laufzeit, _Seat, _mrr, _Aktivierung, _Push, _Airtime):
    _url = 'https://hooks.zapier.com/hooks/catch/16731218/uo1r2ka/'
    _data = {
        'id': _id,
        "carrier": _carrier,
        "Laufzeit": _Laufzeit,
        "Seat": _Seat,
        "mrr": _mrr,
        "Aktivierung": _Aktivierung,
        "Push": _Push,
        "Airtime": _Airtime
    }
    requests.post(url=_url, data=_data)

    # def edit_Provsion_Partner()

    # def edit_Provsion_Carrier()

    # def find_Provsion_Carrier()

    # def find_Provsion_Partner()

    # def check_Provsion_PartnerCarrier()


def Send_Provsion_RowServiceZapier(_id, _Abschluss, _Airtime):
    _url = 'https://hooks.zapier.com/hooks/catch/16731218/uow38ld/'
    _data = {
        'id': _id,
        "Abschluss": _Abschluss,
        "Airtime": _Airtime
    }
    requests.post(url=_url, data=_data)

    # def edit_Provsion_Partner()

    # def edit_Provsion_Carrier()

    # def find_Provsion_Carrier()

    # def find_Provsion_Partner()

    # def check_Provsion_PartnerCarrier()


def Update_Provision_FullCarrier(_carrier, _mrr, _Seat, _Aktivierung, _Push, _Airtime):

    _query_data(
        f"UPDATE mysql.ProvisionCarrier SET  MRR={_mrr[0]}, Seat={_Seat[0]}, Aktivierung = {_Aktivierung[0]}, Push = {_Push[0]}, Airtime = {_Airtime[0]} WHERE carrier = '{_carrier}' AND  Laufzeit = '24 Monate'    ")
    _query_data(
        f"UPDATE mysql.ProvisionCarrier SET  MRR={_mrr[1]}, Seat={_Seat[1]}, Aktivierung = {_Aktivierung[1]}, Push = {_Push[1]}, Airtime = {_Airtime[1]} WHERE carrier = '{_carrier}' AND  Laufzeit = '36 Monate'    ")
    _query_data(
        f"UPDATE mysql.ProvisionCarrier SET  MRR={_mrr[2]}, Seat={_Seat[2]}, Aktivierung = {_Aktivierung[2]}, Push = {_Push[2]}, Airtime = {_Airtime[2]} WHERE carrier = '{_carrier}' AND  Laufzeit = '48 Monate'    ")
    _query_data(
        f"UPDATE mysql.ProvisionCarrier SET  MRR={_mrr[3]}, Seat={_Seat[3]}, Aktivierung = {_Aktivierung[3]}, Push = {_Push[3]}, Airtime = {_Airtime[3]} WHERE carrier = '{_carrier}' AND  Laufzeit = '60 Monate'    ")


def Update_Provision_FullPartner(_carrier, _mrr, _Seat, _Aktivierung, _Push, _Airtime):

    _query_data(
        f"UPDATE mysql.ProvisionPartner SET  MRR={_mrr[0]}, Seat={_Seat[0]}, Aktivierung = {_Aktivierung[0]}, Push = {_Push[0]}, Airtime = {_Airtime[0]} WHERE carrier = '{_carrier}' AND  Laufzeit = '24 Monate'    ")
    _query_data(
        f"UPDATE mysql.ProvisionPartner SET  MRR={_mrr[1]}, Seat={_Seat[1]}, Aktivierung = {_Aktivierung[1]}, Push = {_Push[1]}, Airtime = {_Airtime[1]} WHERE carrier = '{_carrier}' AND  Laufzeit = '36 Monate'    ")
    _query_data(
        f"UPDATE mysql.ProvisionPartner SET  MRR={_mrr[2]}, Seat={_Seat[2]}, Aktivierung = {_Aktivierung[2]}, Push = {_Push[2]}, Airtime = {_Airtime[2]} WHERE carrier = '{_carrier}' AND  Laufzeit = '48 Monate'    ")
    _query_data(
        f"UPDATE mysql.ProvisionPartner SET  MRR={_mrr[3]}, Seat={_Seat[3]}, Aktivierung = {_Aktivierung[3]}, Push = {_Push[3]}, Airtime = {_Airtime[3]} WHERE carrier = '{_carrier}' AND  Laufzeit = '60 Monate'    ")
