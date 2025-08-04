
import base64
import requests
import pandas as pd
import streamlit as st
from mysql.connector import Error
from typing import Optional, Tuple, Any
from lib.db import DatabaseConnection, fetch_data, query_data, _query_data, _fetch_data
from lib.Comparison import ComparisonRating
from lib.Comparison import ComparisonState
from lib.ProduktQ_R_B import ProduktQ_R_B
from lib.Question import Ask_Question, Ask_Anzahl
import datetime
from lib.log import log_User_AngebotLaden, log_User_BewertungAktualisieren, log_User_FromularLaden, log_User_FromularReset, log_User_FromularSenden, log_User_FromularSpeichern

from datetime import datetime
import time


def decode(_str):
    _1 = r'\xc3\xa4'
    _2 = r'\xc3\xb6'
    _3 = r'\xc3\xbc'
    _4 = r'\xc3\x84'
    _5 = r'\xc3\x96'
    _6 = r'\xc3\x9c'
    _7 = r'\xc3\x9f'
    _list = {
        "ä": _1,
        "ö": _2,
        "ü": _3,
        "Ä": _4,
        "Ö": _5,
        "Ü": _6,
        "ß": _7
    }

    for keys, values in _list.items():
        if values in _str:
            _str = _str.replace(values, keys)

    return _str


def make_comp_data(file_path, name):

    df = pd.DataFrame(pd.read_excel(
        file_path, header=0))  # openpyxl

    # Using unique temporary values
    df[name] = df[name].str.replace(
        'g', 'TEMP_G')
    df[name] = df[name].str.replace(
        'n', 'TEMP_N')
    df[name] = df[name].str.replace(
        'm', 'TEMP_M')
    df[name] = df[name].str.replace(
        's', 'TEMP_S')

    df[name] = df[name].str.replace(
        'TEMP_G', "JA")
    df[name] = df[name].str.replace(
        'TEMP_M', "JA")
    df[name] = df[name].str.replace(
        'TEMP_N', "NEIN")
    df[name] = df[name].str.replace(
        'TEMP_S', "JA")

    the_dict = df.set_index('Q_nmb')[name].to_dict()

    return (the_dict)


def make_rat_data(file_path, name):
    df = pd.DataFrame(pd.read_excel(
        file_path, header=0))  # openpyxl

    # Using unique temporary values
    df[name] = df[name].str.replace(
        'g', 'TEMP_G')
    df[name] = df[name].str.replace(
        'n', 'TEMP_N')
    df[name] = df[name].str.replace(
        'm', 'TEMP_M')
    df[name] = df[name].str.replace(
        's', 'TEMP_S')

    df[name] = df[name].str.replace(
        'TEMP_G', "gut")
    df[name] = df[name].str.replace(
        'TEMP_M', "mittel")
    df[name] = df[name].str.replace(
        'TEMP_N', "nicht möglich")
    df[name] = df[name].str.replace(
        'TEMP_S', "schlecht")

    the_dict = df.set_index('Q_nmb')[name].to_dict()

    return (the_dict)


def take_data(file_path, name):
    df = pd.DataFrame(pd.read_excel(
        file_path, header=0))  # openpyxl

    the_dict = df.set_index('Q_nmb')[name].to_dict()

    return (the_dict)


def compare_dicts(dict1, dict2):
    """
    Compare two dictionaries with ComparisonState values.
    Returns different only when dict1 has JA and dict2 has NEIN.
    All other combinations are considered matching.

    Args:
        dict1 (dict): First dictionary with ComparisonState values
        dict2 (dict): Second dictionary with ComparisonState values

    Returns:
        dict: Dictionary with same keys and values indicating "same" or "different"
    """
    result = {}

    # Check if both dictionaries have the same keys
    if set(dict1.keys()) != set(dict2.keys()):
        raise ValueError("Dictionaries must have the same keys")

    # Compare values for each key
    for key in dict1:
        # Only different if dict1 is JA and dict2 is NEIN
        is_different = (dict1[key] == "JA" and
                        dict2[key] == "NEIN")
        result[key] = "different" if is_different else "same"

    return result


def calculate_sim(_dict):
    counter = 0
    counter_total = 0
    for values in _dict:
        if values == "same":
            counter += 1
            counter_total += 1
    result = counter / counter_total * 100
    result = round(result, 2)
    result = format(result, )
    return result


def make_comp_data_SQL(name):
    with DatabaseConnection(
        host="85.215.198.141",
        user="webapp",
        database="mysql",
        password="vv_webapp_2025"
    ) as db:
        # Execute multiple queries
        data = db.execute_query(
            "SELECT NFON_Business_Premium FROM mysql.Karrier_tb")

        df = pd.DataFrame(data)

    # Using unique temporary values
    df[name] = df[name].str.replace(
        'g', 'TEMP_G')
    df[name] = df[name].str.replace(
        'n', 'TEMP_N')
    df[name] = df[name].str.replace(
        'm', 'TEMP_M')
    df[name] = df[name].str.replace(
        's', 'TEMP_S')

    df[name] = df[name].str.replace(
        'TEMP_G', "JA")
    df[name] = df[name].str.replace(
        'TEMP_M', "JA")
    df[name] = df[name].str.replace(
        'TEMP_N', "NEIN")
    df[name] = df[name].str.replace(
        'TEMP_S', "JA")

    the_dict = df.set_index('Q_nmb')[name].to_dict()

    return (the_dict)


def make_comp_SQL(name):
    with DatabaseConnection(
        host="85.215.198.141",
        user="webapp",
        database="mysql",
        password="vv_webapp_2025"
    ) as db:
        # Execute multiple queries
        data = db.execute_query(
            "SELECT OneAndOne_Business_Phone FROM mysql.Karrier_tb")
        df = pd.DataFrame(data)

    df[name] = df[name].str.replace(
        'g', 'TEMP_G')
    df[name] = df[name].str.replace(
        'n', 'TEMP_N')
    df[name] = df[name].str.replace(
        'm', 'TEMP_M')
    df[name] = df[name].str.replace(
        's', 'TEMP_S')

    df[name] = df[name].str.replace(
        'TEMP_G', "JA")
    df[name] = df[name].str.replace(
        'TEMP_M', "JA")
    df[name] = df[name].str.replace(
        'TEMP_N', "NEIN")
    df[name] = df[name].str.replace(
        'TEMP_S', "JA")


def sort_products_by_score(product_dict):
    """
    Sorts a nested product dictionary by the numeric scores (at index 1 of each inner list),
    while preserving the pointers to class instances.

    Args:
        product_dict (dict): A dictionary where each key is a product name and each value
                            is a list containing a ProduktQ_R_B object at index 0 and a score at index 1

    Returns:
        dict: A new sorted dictionary with the same structure but ordered by descending score
    """
    # Sort the items by the score (index 1) in descending order
    sorted_items = sorted(product_dict.items(),
                          key=lambda item: item[1][1], reverse=True)

    # Create a new dictionary from the sorted items
    sorted_dict = dict(sorted_items)

    return sorted_dict


def product_name():
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()
    sql_list = db.execute_query(
        """SELECT Produkt_display_name, Produkt_name FROM mysql.Karrier_aktive_tb""")
    db.disconnect()
    result_dict = {}
    for items in sql_list:
        result_dict[[items][0]["Produkt_display_name"]] = [
            items][0]["Produkt_name"]

    return result_dict


def product_display_name():
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()
    sql_list = db.execute_query(
        """SELECT Produkt_display_name, Produkt_name FROM mysql.Karrier_aktive_tb""")
    db.disconnect()
    result_dict = {}
    for items in sql_list:
        result_dict[[items][0]["Produkt_name"]] = [
            items][0]["Produkt_display_name"]

    return result_dict


def tag_produkt(curremt_user):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()
    liste = db.execute_query(
        f"""SELECT * FROM mysql.produkt_tag_tb WHERE tag = '{curremt_user}'
    """)
    db.disconnect()
    Produkt_aktive = []
    for items in liste:
        for produkt in [items][0].keys():
            if (items[produkt]) == 1:
                Produkt_aktive.append(produkt)
    return Produkt_aktive


def make_rating_dict(full_awnser_list, kunden_daten):
    dict_P = {}
    product_display_key = product_display_name()
    User_access_list = tag_produkt(
        st.session_state.current_role)  # .name sind display name

    veovia_lead_quelle_compatibility = {
        "Alt-Bestand": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Bestand Home": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Avaya Neu": ["Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate"],
        "Avaya-Bestand": ["Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate"],
        "Avaya.de Abkündigung": ["Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate"],
        "bedirekt": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Deutsche Glasfaser": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "DHO": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Digital Phone": ["Digital_Phone_Business"],
        "DNS:NET": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "DNS:NET Partner": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "DTS-Systems Partner": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Google Plus": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Hunter Solution": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Inbound Call": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Live-Akquise": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Kundenempfehlung": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "LinkedIn": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "marketingmanufaktur": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Messe": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "MHWK": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "MVF": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "NFON": ["NFON_Business_Premium", "Digital_Phone_Business", "WTG_CLOUD_PURE"],
        "NFON-Direct-Sales": ["NFON_Business_Premium", "Digital_Phone_Business"],
        "NFON-Partner": ["NFON_Business_Premium", "Digital_Phone_Business"],
        "persönlicher Kontakt": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Placatel": ["Placetel_Profi"],
        "RLA": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Telefonica": ["Digital_Phone_Business"],
        "Telemarketing": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "TK-Vergleich": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Trading Twins": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Vodafone Direct Sales": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "VVL": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Webformular": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "WHBTA": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Zusatzgeschäft": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "And Friends": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "ASC-MV": ["NFON_Business_Premium", "Digital_Phone_Business", "WTG_CLOUD_PURE"],
        "WTG": ["WTG_CLOUD_PURE"],
        "NFON Ablöse": ["Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE", "Eins_und_Eins_Business_Phone"]
    }

    standart_lead_quelle_compatibilityt = {
        "Alt-Bestand": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Bestand Home": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Avaya Neu": ["Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate"],
        "Avaya-Bestand": ["Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate"],
        "Avaya.de Abkündigung": ["Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate"],
        "bedirekt": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Deutsche Glasfaser": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "DHO": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Digital Phone": ["Digital_Phone_Business"],
        "DNS:NET": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "DNS:NET Partner": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "DTS-Systems Partner": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Google Plus": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Hunter Solution": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Inbound Call": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Live-Akquise": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Kundenempfehlung": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "LinkedIn": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "marketingmanufaktur": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Messe": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "MHWK": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "MVF": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "NFON": ["NFON_Business_Premium", "Digital_Phone_Business", "WTG_CLOUD_PURE"],
        "NFON-Direct-Sales": ["NFON_Business_Premium", "Digital_Phone_Business"],
        "NFON-Partner": ["NFON_Business_Premium", "Digital_Phone_Business"],
        "persönlicher Kontakt": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Placatel": ["Placetel_Profi"],
        "RLA": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Telefonica": ["Digital_Phone_Business"],
        "Telemarketing": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "TK-Vergleich": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Trading Twins": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Vodafone Direct Sales": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "VVL": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Webformular": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "WHBTA": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "Zusatzgeschäft": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "And Friends": ["NFON_Business_Premium", "Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE",  "Eins_und_Eins_Business_Phone"],
        "ASC-MV": ["NFON_Business_Premium", "Digital_Phone_Business", "WTG_CLOUD_PURE"],
        "WTG": ["WTG_CLOUD_PURE"],
        "NFON Ablöse": ["Avaya_Cloud_Office_Standard", "Avaya_Cloud_Office_Premium", "Avaya_Cloud_Office_Ultimate", "ecotel_cloud_phone", "Digital_Phone_Business", "WTG_CLOUD_PURE", "Eins_und_Eins_Business_Phone"]
    }
    # gives a list of product on for this lead
    produkt_liste = []

    for item in User_access_list:
        if st.session_state.current_role == "veovia":
            if item in veovia_lead_quelle_compatibility.get(st.session_state.K2):
                try:
                    produkt_liste.append(product_display_key[item])
                except KeyError:
                    print("")
                else:
                    produkt_liste.append(product_display_key[item])
        else:
            if item in standart_lead_quelle_compatibilityt.get(st.session_state.K2):
                try:
                    produkt_liste.append(product_display_key[item])
                except KeyError:
                    print("")
                else:
                    produkt_liste.append(product_display_key[item])

    for i in range(len(ProduktQ_R_B._registry)):
        name = ProduktQ_R_B._registry[i].name
        if name in produkt_liste:
            answer = ProduktQ_R_B._registry[i].compaire(
                full_awnser_list).items()
            rate = ProduktQ_R_B._registry[i].make_rating(input=answer)
            dict_P[name] = rate
    return dict_P


def get_Q_list_SQL(name: str):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )

    db.connect()

    df_result = pd.DataFrame(db.execute_query(
        query="SELECT " f'{name}'", Q_nmb FROM mysql.Karrier_tb "

    ))
    db.disconnect()

    # Using unique temporary values
    df_result[name] = df_result[name].str.replace(
        'g', 'TEMP_G')
    df_result[name] = df_result[name].str.replace(
        'n', 'TEMP_N')
    df_result[name] = df_result[name].str.replace(
        'm', 'TEMP_M')
    df_result[name] = df_result[name].str.replace(
        's', 'TEMP_S')

    df_result[name] = df_result[name].str.replace(
        'TEMP_G', "JA")
    df_result[name] = df_result[name].str.replace(
        'TEMP_M', "JA")
    df_result[name] = df_result[name].str.replace(
        'TEMP_N', "NEIN")
    df_result[name] = df_result[name].str.replace(
        'TEMP_S', "JA")

    the_dict = df_result.set_index('Q_nmb')[name].to_dict()
    finall_dict = {f'Q{k}': v for k, v in the_dict.items()}
    return finall_dict


def get_R_list_SQL(name: str):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )

    db.connect()

    df_result = pd.DataFrame(db.execute_query(
        query="SELECT " f'{name}'", Q_nmb FROM mysql.Karrier_tb "

    ))
    db.disconnect()

    # Using unique temporary values
    df_result[name] = df_result[name].str.replace(
        'g', 'TEMP_G')
    df_result[name] = df_result[name].str.replace(
        'n', 'TEMP_N')
    df_result[name] = df_result[name].str.replace(
        'm', 'TEMP_M')
    df_result[name] = df_result[name].str.replace(
        's', 'TEMP_S')

    df_result[name] = df_result[name].str.replace(
        'TEMP_G', "gut")
    df_result[name] = df_result[name].str.replace(
        'TEMP_M', "mittel")
    df_result[name] = df_result[name].str.replace(
        'TEMP_N', "nicht möglich")
    df_result[name] = df_result[name].str.replace(
        'TEMP_S', "schlecht")

    the_dict = df_result.set_index('Q_nmb')[name].to_dict()
    finall_dict = {f'Q{k}': v for k, v in the_dict.items()}
    return finall_dict


def get_B_list_SQL(name: str):

    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()

    df_result = pd.DataFrame(db.execute_query(
        query="SELECT " f'{name}'", Q_nmb FROM mysql.Karrier_tb "

    ))
    db.disconnect()

    the_dict = df_result.set_index('Q_nmb')[name].to_dict()
    finall_dict = {f'Q{k}': v for k, v in the_dict.items()}
    return finall_dict


def make_side_bar(full_awnser_list, kunden_daten):
    '''
    Nimmt die Antworten und Erstellt die Bewertung in der Sidebar
    '''

    with st.sidebar:
        col1, col2 = st.columns([0.7, 0.3], vertical_alignment="bottom")
        with col1:
            st.markdown("<h1 style='font-size: 30px;'>Produkt Bewertungen</h1>",
                        unsafe_allow_html=True)
        with col2:
            st.button("Aktualisieren", key="side_bar_Aktualisieren",
                      on_click=log_User_BewertungAktualisieren)

        sorted_ratings = {}

        sorted_ratings = sort_products_by_score(
            make_rating_dict(full_awnser_list, kunden_daten))
        # Title
        for key, values in sorted_ratings.items():

            # Wichtig & Nicht möglich
            Ausschlusskriterium = False
            for Q, val in values[0].compaire(full_awnser_list).items():
                if val == "KO":
                    Ausschlusskriterium = True

            # Optional & Nicht möglich
            B_OpN = False
            for Q, val in values[0].compaire(full_awnser_list).items():
                if val == "NO":
                    B_OpN = True

            # wichtig & Mittel und Optional & Mittel
            Mittel = False
            for Q, val in values[0].compaire(full_awnser_list).items():
                if val == "MJ" or val == "MO":
                    Mittel = True

            # #ff0000 --> red
            st.markdown("""
                <style>
                    .header-part-1 {
                        color: red;
                        font-size: 20px;
                        display: inline;
                    }
                    .header-part-2 {
                        color: green;
                        font-size: 20px;
                        display: inline;
                    }
                    .header-part-3 {
                        color: black;
                        font-size: 20px;
                        display: inline;
                    }


                    .header-part-4 {
                        color: green;
                        font-size: 17px;
                        display: inline;
                    }
                    .header-part-5 {
                        color: orange;
                        font-size: 17px;
                        display: inline;
                    }
                    .header-part-6 {
                        color: red;
                        font-size: 17px;
                        display: inline;
                    }


                    .header-part-7 {
                        color: black;
                        font-size: 17px;
                        display: inline;
                    }

                    .header-container {
                        border-bottom: 2px solid #506e73;
                    }
                    .p-1{
                        color: red;
                        font-size: 14px;
                        line-height: 0.2;
                    }
                    .p-2{
                        color: black;
                        font-size: 14px;
                        line-height: 0.2;
                    }
                </style>
                """, unsafe_allow_html=True)

            if Ausschlusskriterium == True:
                # <div class="header-part-1"> and <div class="header-part-2">

                st.markdown("""<div class="header-container"><span class="header-part-1">nicht möglich &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span> <span class="header-part-3">"""f'  - &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; {key}'"""</span></div>""",
                            unsafe_allow_html=True)
            if Ausschlusskriterium == False:
                if round(values[1], 1) < 100:
                    st.markdown(
                        """<div class="header-container"><span class="header-part-2">"""f'{format(round(values[1], 1))}% &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'"""</span> <span class="header-part-3">"""f'  - &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; {key}'"""</span></div>""", unsafe_allow_html=True)
                elif round(values[1], 2) == 100:
                    st.markdown(
                        """<div class="header-container"><span class="header-part-2">"""f'{format(round(values[1], 1))}% &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'"""</span> <span class="header-part-3">"""f'  - &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; {key}'"""</span></div>""", unsafe_allow_html=True)

                st.markdown("")
            #
            if Ausschlusskriterium == True:
                for Q, val in values[0].compaire(full_awnser_list).items():
                    if val == "KO":
                        for items in range(len(Ask_Question._registry)):
                            if Ask_Question._registry[items].key == Q:
                                frage_ausgeschrieben = Ask_Question._registry[items].name
                        string_to_write = frage_ausgeschrieben
                        help_string = values[0].B_list.get(Q)
                        st.markdown(
                            """<span class="p-1">"""f'{string_to_write}'"</span>""", unsafe_allow_html=True, help=help_string)

            # BEWERTUNG
            B_Gut = False
            for Q, val in values[0].compaire(full_awnser_list).items():
                if val == "GJ" or val == "GO":
                    B_Gut = True

            B_Mittel = False
            for Q, val in values[0].compaire(full_awnser_list).items():
                if val == "MJ" or val == "MO":
                    B_Mittel = True

            B_Schlecht = False
            for Q, val in values[0].compaire(full_awnser_list).items():
                if val == "SJ" or val == "SO":
                    B_Schlecht = True

            if B_Gut == True or B_Mittel == True or B_Schlecht == True or B_OpN == True:
                with st.expander("Bewertung: "):
                    if B_Gut == True:
                        # Funktionen erfüllt
                        st.markdown(
                            """<div class="header-container"><span class="header-part-4">Funktionen erfüllt:</span>""", unsafe_allow_html=True)

                        for Q, R in values[0].compaire(full_awnser_list).items():
                            for items in range(len(Ask_Question._registry)):
                                if Ask_Question._registry[items].key == Q:
                                    frage_ausgeschrieben = Ask_Question._registry[items].name
                            if R == "GJ":
                                string_to_write = frage_ausgeschrieben

                                st.markdown(
                                    """<span class="p-2">"""f'{string_to_write}'"</span>""", unsafe_allow_html=True, help=values[0].B_list.get(Q))

                            if R == "GO":
                                string_to_write = frage_ausgeschrieben + \
                                    " (Optional)"
                                st.markdown(
                                    """<span class="p-2">"""f'{string_to_write}'"</span>""", unsafe_allow_html=True, help=values[0].B_list.get(Q))
                    if B_Mittel == True:
                        st.markdown(
                            """<div class="header-container"><span class="header-part-5">Mäßig erfüllt:</span>""", unsafe_allow_html=True)
                        for Q, R in values[0].compaire(full_awnser_list).items():
                            for items in range(len(Ask_Question._registry)):
                                if Ask_Question._registry[items].key == Q:
                                    frage_ausgeschrieben = Ask_Question._registry[items].name
                            if R == "MJ":
                                string_to_write = frage_ausgeschrieben
                                st.markdown(
                                    """<span class="p-2">"""f'{string_to_write}'"</span>""", unsafe_allow_html=True, help=values[0].B_list.get(Q))
                            if R == "MO":
                                string_to_write = frage_ausgeschrieben + \
                                    " (Optional)"
                                st.markdown(
                                    """<span class="p-2">"""f'{string_to_write}'"</span>""", unsafe_allow_html=True, help=values[0].B_list.get(Q))

                    if B_Schlecht == True:
                        st.markdown(
                            """<div class="header-container"><span class="header-part-6">Schlecht erfüllt:</span>""", unsafe_allow_html=True)
                        for Q, R in values[0].compaire(full_awnser_list).items():
                            for items in range(len(Ask_Question._registry)):
                                if Ask_Question._registry[items].key == Q:
                                    frage_ausgeschrieben = Ask_Question._registry[items].name
                            if R == "SJ":
                                string_to_write = frage_ausgeschrieben
                                st.markdown(
                                    """<span class="p-2">"""f'{string_to_write}'"</span>""", unsafe_allow_html=True, help=values[0].B_list.get(Q))
                            if R == "SO":
                                string_to_write = frage_ausgeschrieben + \
                                    " (Optional)"
                                st.markdown(
                                    """<span class="p-2">"""f'{string_to_write}'"</span>""", unsafe_allow_html=True, help=values[0].B_list.get(Q))
                    if B_OpN == True:
                        st.markdown(
                            """<div class="header-container"><span class="header-part-7">Funktionen nicht erfüllt:</span>""", unsafe_allow_html=True)
                        for Q, val in values[0].compaire(full_awnser_list).items():
                            if val == "NO":
                                for items in range(len(Ask_Question._registry)):
                                    if Ask_Question._registry[items].key == Q:
                                        frage_ausgeschrieben = Ask_Question._registry[items].name
                                        string_to_write = frage_ausgeschrieben + \
                                            " (Optional)"
                                        help_string = values[0].B_list.get(Q)
                                        st.markdown(
                                            """<span class="p-2">"""f'{string_to_write}'"</span>""", unsafe_allow_html=True, help=values[0].B_list.get(Q))


def Get_ID_SQL():
    querry = """
SELECT MAX(ID) as MAX_ID From mysql.Angebot_Anfrage
        """
    ID = int(query_data(querry))
    return ID


def format_for_zapier(full_awnser_list_S, kunden_daten):
    '''
    Formatiert und sendet die Antwort-Daten an Zapier
    '''

    def get_firmen_partner_details():
        '''
        Firmenname, strasse, plz, ort, primary_Farbe, secondary_Farbe, primary_font_Farbe, secondary_font_Farbe, partner_status, partner_id, zoho_nutzer_id, firmen_tag
        '''
        daten = _fetch_data(
            f"SELECT * FROM mysql.Partner_details WHERE firmen_tag = '{st.session_state.current_role}' ")

        return daten[0].get("Firmenname"), daten[0].get("strasse"), daten[0].get("plz"), daten[0].get("ort"), daten[0].get("primary_Farbe"), daten[0].get("secondary_Farbe"), daten[0].get("primary_font_Farbe"), daten[0].get("secondary_font_Farbe"), daten[0].get("partner_status"), daten[0].get("partner_id"), daten[0].get("zoho_nutzer_id"), daten[0].get("firmen_tag")

    webhook_url = 'https://hooks.zapier.com/hooks/catch/16731218/20o1kyf/'

    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )

    db.connect()

    # id = db.execute_query("SELECT MAX(id) FROM mysql.form_tb")

    if st.session_state.S90 == True:
        kleine_Telefone_Optional = "optional"
    else:
        kleine_Telefone_Optional = None

    if st.session_state.S91 == True:
        mittlere_Telefone_Optional = "optional"
    else:
        mittlere_Telefone_Optional = None

    if st.session_state.S92 == True:
        grosse_Telefone_Optional = "optional"
    else:
        grosse_Telefone_Optional = None

    if st.session_state.S93 == True:
        Netzteile_Optional = "optional"
    else:
        Netzteile_Optional = None

    if st.session_state.S94 == True:
        Expansionsmodule_Optional = "optional"
    else:
        Expansionsmodule_Optional = None

    if st.session_state.S95 == True:
        Konferenztelefon_Optional = "optional"
    else:
        Konferenztelefon_Optional = None

    Grund_ = ""
    if st.session_state.K5 == True:
        Grund_ += "Erstanschaffung, "
    if st.session_state.K6 == True:
        Grund_ += "Aktuelle Anlage defekt, "
    if st.session_state.K7 == True:
        Grund_ += "Homeoffice-Funktionen benötigt, "
    if st.session_state.K8 == True:
        Grund_ += "IT-Systeme integrieren, "
    if st.session_state.K9 == True:
        Grund_ += "Alter aktuelle Anlage, "
    if st.session_state.K10 == True:
        Grund_ += "Andere, "
    if st.session_state.K11 is not None:
        Grund_ += "Genauere Beschreibung" + format(st.session_state.K11, "")

    App = ""
    Web = ""
    if st.session_state.S80 == True:
        App = "App"

    if st.session_state.S81 == True:
        Web = "WebRTC"

    if "Standort - Adresszeile 1" not in st.session_state:
        st.session_state["Standort - Adresszeile 1"] = ""
    if "Standort - Adresszeile 2" not in st.session_state:
        st.session_state["Standort - Adresszeile 2"] = ""
    if "Standort - Postleitzahl" not in st.session_state:
        st.session_state["Standort - Postleitzahl"] = ""
    if "Standort - Land" not in st.session_state:
        st.session_state["Standort - Land"] = ""
    if "Standort - Adresszeile 1" not in st.session_state:
        st.session_state["Standort - Stadt"] = ""

    data = {
        # User DATEN
        "Vorname": decode(st.session_state.current_Vorname),
        "Nachname": decode(st.session_state.current_Nachname),
        "Email": st.session_state.current_Kontak_email,
        "Telefon": st.session_state.current_Telefonnummer_mobil,
        "Telefonnummer festnetzt": st.session_state.current_Telefonnummer_Festnetzt,

        # KUNDEN DATEN
        "Leadquelle": decode(st.session_state.K2),
        "Kontaktname": decode(st.session_state.K1),
        "Abschlussdatum(voraussichtlich)": format(st.session_state.K3, ),
        "Startdatum": format(st.session_state.K4, ),
        "Grund für neue Telefonanlage": decode(Grund_),

        # Nutzer
        "AnzahlSeats": st.session_state.S1,
        "Anzahl Nutzer Windows-Client": st.session_state.S18,
        "Anzahl Nutzer macOS-Client": st.session_state.S19,
        "Anzahl iOS-Nutzer": st.session_state.S26,
        "Anzahl Android-Nutzer": st.session_state.S27,
        "Anzahl Sprachkanäle": st.session_state.S2,
        "Anzahl eFax": st.session_state.S37,
        "Anzahl analoge Faxgeräte": st.session_state.S38,
        "Anzahl Türöffner": st.session_state.S39,
        "Anzahl nur Hardware Telefon": st.session_state.S56,
        "Nutzung auf Computer per": f"{App}, {Web}",
        "Windows Softwarebereitstellung": st.session_state.S21,

        # Grundlegendes
        "Spezifische Laufzeit": st.session_state.Q4,
        "Laufzeit in Montaten": st.session_state.S55,
        "Wartungsvertrag": st.session_state.S5,
        "Video & Chat": f"Video & Conferencing {st.session_state.Q55} Chat: {st.session_state.Q11}",


        # Rufnummern & Flatrates

        "Anzahl neue 10er Rufnummernblöcke": st.session_state.S6,
        "Flatrates": f"Flatrate DE Festnetz:{st.session_state.Q18} Flatrate DE Mobilfunk:{st.session_state.Q19} Flatrate EU Festnetz:{st.session_state.Q20} Flatrate EU Mobilfunk:{st.session_state.Q21}",
        "Portierung - Anzahl Rufnummernblöcke": st.session_state.S7,
        "Portierung - Anzahl Einzelrufnummern / MSNs": st.session_state.S8,

        # Sicherheit
        "Sicherheit / Grundlegendes": f"Unteranlagen:{st.session_state.Q53} Benutzer-/Rollenkonzept für Administration:{st.session_state.Q8} Deutscher Hersteller:{st.session_state.Q16} Deutsche Rechenzentren:{st.session_state.Q15} Sperren bestimmter Rufnummern (eingehend):{st.session_state.Q46} Gesprächsverschlüsselung:{st.session_state.Q23} Single Sign-On (SSO):{st.session_state.Q43} Clip no Screening (CNS):{st.session_state.Q13} Softwarerollout per .msi:{st.session_state.Q45}",

        # Integration
        "Integrationen": f"API - Einrichtung/System:{st.session_state.Q7} Active Directory - Benutzersynchronisierung:{st.session_state.Q1}",

        # FRAGEN - Contact Center & Analyse
        "Analyse & Contact Center": f"Contact Center:{st.session_state.Q14} Sprachaufzeichnung - automatisiert:{st.session_state.Q48} Sprachaufzeichnung - on demand:{st.session_state.Q49} Nachträgliche Analyse:{st.session_state.Q31} Live-Analyse:{st.session_state.Q27} Qualitätsmessung von Calls:{st.session_state.Q39}",

        # ANZAHL - Contact Center & Analyse
        "Contact Center - Anzahl Nutzer": st.session_state.S58,
        "Sprachaufzeichnung - Anzahl Nutzer": st.session_state.S10,
        "Nachträgliche Analyse Nutzer": st.session_state.S57,
        "Nachträgliche Analyse Supervisoren": st.session_state.S11,
        "Live-Analyse Nutzer": st.session_state.S12,
        "Live-Analyse Supervisoren": st.session_state.S13,
        "Contact Center": st.session_state.Q14,
        # FRAGEN - Anrufhandling
        "Anrufhandling": f"Sprachansagen / IVRs:{st.session_state.Q47} Anklopfen:{st.session_state.Q3} Gruppen / Warteschlangen:{st.session_state.Q24} Warteschlangen-Überlauf:{st.session_state.Q58} Routingoptionen für Warteschlangen:{st.session_state.Q40} Weiterleitungen aus Warteschlangen / Gruppen zu internen Zielen:{st.session_state.Q61} Weiterleitungen aus Warteschlangen / Gruppen zu externen Zielen:{st.session_state.Q60} Pickupgruppen aus Nebenstellen:{st.session_state.Q37} Weiterleitung aus Nebenstellen:{st.session_state.Q59}  Rufumleitungsprofile:{st.session_state.Q41} Chef-Sek-Funktion:{st.session_state.Q12} Voicemail:{st.session_state.Q56} Voicemail-Transkription:{st.session_state.Q57} Zeitsteuerung:{st.session_state.Q62}",
        # ANZAHL - Anrufhandling
        "Anzahl Gruppen/Warteschlangen von extern erreichbar": st.session_state.S14,
        "Anzahl Sprachdialoge/IVRs von extern erreichbar": st.session_state.S16,

        # Grundfunktionen
        "Grundfunktionen": f"Besetztlampenfelder:{st.session_state.Q9} Hotdesking / Free Seating:{st.session_state.Q25}  Zentrales Telefonbuch:{st.session_state.Q63} Kurzwahlziel extern:{st.session_state.Q26} Anrufhistorie für eigene Anrufe:{st.session_state.Q5} Anrufhistorie für alle Anrufe:{st.session_state.Q4} Gegensprechfunktion / Interkom:{st.session_state.Q22} Bitte nicht stören (DND):{st.session_state.Q10} Mehr als 1 Gerät pro Nebenstelle:{st.session_state.Q29} Mehr als 1 Durchwahl pro Nebenstelle:{st.session_state.Q28} Vermittlungsarbeitsplatz:{st.session_state.Q54} Telefonkonferenzen:{st.session_state.Q51}",

        # FRAGEN - Microsoft Teams
        "Microsoft Teams": f"MS Teams Direct Routing:{st.session_state.Q30} Dialer in MS Teams:{st.session_state.Q17} Präsenzabgleich MS Teams:{st.session_state.Q38}",
        # ANZAHL - Microsoft Teams
        "Anzahl Nutzer MS Teams, zusätzlich zu Nebenstelle": st.session_state.S22,
        "Anzahl Nutzer MS Teams - eigene Nebenstelle": st.session_state.S23,
        "Anzahl Dialer-Verwendung in MS Teams": st.session_state.S24,
        "Anzahl Präsenzabgleich zu MS Teams": st.session_state.S25,
        "Nebenstelle auf dem Smartphone - Android": st.session_state.Q34,
        "Nebenstelle auf dem Smartphone - iOS": st.session_state.Q35,

        # Hardware Telefone
        "Anzahl Kleine Telefone": st.session_state.S28,
        "Kleine Telefone Optional": kleine_Telefone_Optional,

        "Anzahl Mittlere Telefone": st.session_state.S29,
        "Mittlere Telefone Optional": mittlere_Telefone_Optional,

        "Anzahl Grosse Telefone": st.session_state.S30,
        "Grosse Telefone Optional": grosse_Telefone_Optional,

        "Anzahl Netzteile": st.session_state.S31,
        "Anzahl Netzteile Optional": Netzteile_Optional,

        "Anzahl Expansionsmodule": st.session_state.S32,
        "Anzahl Expansionsmodule Optional": Expansionsmodule_Optional,

        "Anzahl Konferenztelefon": st.session_state.S33,
        "Anzahl Konferenztelefon Optional": Konferenztelefon_Optional,


        # DECT
        "Anzahl neue DECT-Mobilteile": st.session_state.S4,
        "Anzahl Singlecell - DECT - Basisstationen":  st.session_state.S9,
        "Multicell-DECT-Basisstationen":  None,
        # CTI Funktionen:
        "CTI Funktionen": f"Rufnummernauflösung:{st.session_state.S45} Click-to-Dial aus CRM/ERP:{st.session_state.S46} Erfassung Anrufe in CRM/ERP:{st.session_state.S47} Anruflenkung durch eingehende Nummer:{st.session_state.S48} Anruflenkung durch DTMF:{st.session_state.S49} Akustische Ausgabe von CRM-/ERP-Infos durch DTMF:{st.session_state.S50}",

        # FRAGEN - Anbingung - Integrationen
        "Anbingung - Integrationen": f"""Integration gewünscht?:{st.session_state.S40} Nebenstelle auf dem Computer - App:{st.session_state.Q32} Nebenstelle auf dem Computer - webRTC:{st.session_state.Q33} Slack embedded Dialer:{st.session_state.Q44} TAPI 2.x:{st.session_state.Q50} Zoom:{st.session_state.Q64}""",
        "Wird TAPI von der angegebenen Software unterstützt?": {st.session_state.S44},
        "Welche Software soll angebunden werden?": {st.session_state.S42},
        "CTI-Nutzung auf welchen Geräten?": {st.session_state.S54},
        "CTI - Findet eine spezielle Art der Softwarebereitstellung statt?": {st.session_state.S52},
        "CTI - Welches Betriebssystem ist im Einsatz?": {st.session_state.S51},
        # ANZAHL - Anbingung - Integrationen
        "Anzahl Nutzer Intergration": {st.session_state.S41},

    }

    _transformer = {
        "wichtig": "j",
        "unwichtig": "n",
        "Optional": "o",
        "None": "n"

    }
    fragen_Antworten = []
#    for nummer in range(len(Ask_Anzahl._registry)):
#        if Ask_Anzahl._registry[nummer].key in st.session_state:
#            _val = st.session_state[Ask_Anzahl._registry[nummer].key]
#            _key = Ask_Anzahl._registry[nummer].key
#            _string = str(_key) + ": " + str(_val)
#            fragen_Antworten.append(_string)

    for nummer in range(len(Ask_Question._registry)):
        _val = st.session_state[Ask_Question._registry[nummer].key]
        _key = Ask_Question._registry[nummer].key
        _string = str(_key) + ":" + _transformer.get(_val)

        fragen_Antworten.append(_string)

    data["Key und Antwort"] = fragen_Antworten

    ## nicht veovia ##
    if st.session_state.current_role != "veovia":

        data["Standort - Adresszeile 1"] = st.session_state["Standort - Adresszeile 1"]
        data["Standort - Adresszeile 2"] = st.session_state["Standort - Adresszeile 2"]
        data["Standort - Postleitzahl"] = st.session_state["Standort - Postleitzahl"]
        data["Standort - Land"] = st.session_state["Standort - Land"]
        data["Standort - Stadt"] = st.session_state["Standort - Stadt"]

        if st.session_state.current_role == '7Werk':
            data["partner"] = "7WERK"
        else:
            data["partner"] = st.session_state.current_role

        # veovia
        data["username"] = st.session_state.current_username
        data["veovia"] = False
        data["Zoho_ID_KUNDE"] = "No ID"

        data["KundenName"] = st.session_state.KundenName
        data["Datum"] = datetime.now().date()

    ## veovia ##
    if st.session_state.current_role == "veovia":

        # IF ZOHO ID USE
        if st.session_state.check_passed == True:
            data["Standort - Adresszeile 1"] = st.session_state["Standort - Adresszeile 1"]
            data["Standort - Adresszeile 2"] = st.session_state["Standort - Adresszeile 2"]
            data["Standort - Postleitzahl"] = st.session_state["Standort - Postleitzahl"]
            data["Standort - Land"] = st.session_state["Standort - Land"]
            data["Standort - Stadt"] = st.session_state["Standort - Stadt"]
            data["partner"] = st.session_state.current_role
            # veovia
            data["username"] = st.session_state.current_username
            data["veovia"] = True
            data["Zoho_ID_KUNDE"] = kunden_daten["zoho_id"]

            data["Datum"] = datetime.now().date()

        # IF ZOHO ID USE NOT
        if st.session_state.check_passed != True:

            data["Standort - Adresszeile 1"] = st.session_state["Standort - Adresszeile 1"]
            data["Standort - Adresszeile 2"] = st.session_state["Standort - Adresszeile 2"]
            data["Standort - Postleitzahl"] = st.session_state["Standort - Postleitzahl"]
            data["Standort - Land"] = st.session_state["Standort - Land"]
            data["Standort - Stadt"] = st.session_state["Standort - Stadt"]
            data["partner"] = st.session_state.current_role
            # veovia

            data["username"] = st.session_state.current_username
            data["veovia"] = True
            data["Zoho_ID_KUNDE"] = "NOT USED"

            data["KundenName"] = st.session_state.KundenName
            data["Datum"] = datetime.now().date()

    # sending to SQL
    id = send_dict_to_SQL(grab_auftrag_and_make_dict(
        kunden_daten, full_awnser_list_S))

    # get SQL ID
    chain_ID_to_User(st.session_state.user_id)
    data["Kalkulator_id"] = st.session_state.Kalkulator_id

    _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12 = get_firmen_partner_details()

    data["Partner Firmenname"] = _1
    data["Partner Strasse"] = _2
    data["Partner PLZ"] = _3
    data["Partner Ort"] = _4
    data["Partner Primärfarbe"] = _5
    data["Partner Sekundärfarbe"] = _6
    data["Partner Primär Schriftfarbe"] = _7
    data["Partner Sekundär Schriftfarbe"] = _8
    data["Partner Status"] = _9
    data["Partner ID"] = _10
    data["Zoho Nutzer ID Partner"] = _11

    data["Zoho Nutzer ID Person"] = st.session_state.Zoho_Nutzer_id_Person
    requests.post(webhook_url, data=data)

    return True

# NGROK LINK UPDATE SPOT


def PDF_fetcher(filename):

    def display_pdf(pdf_data, filename):
        # Encode PDF t://o base64
        base64_pdf = base64.b64encode(pdf_data).decode('utf-8')
        st.html("""<style>

            .iframe-container {
                display: flex;
                justify-content: center;
                align-items: flex-start;
                width: 950px;
                min-height: 1200px;
                margin-left: -130px; /* Distance from left edge */
                margin-top: 20px; /* Distance from top edge - adjust this value */
                margin-bottom: 20px;
            }

            .iframe-wrapper {
                border: 2px solid #ddd;
                border-radius: 8px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                overflow: hidden;
                transform: scale(1.0);
            }

            iframe {
                display: block;
                border: none;

            }
        </style>
    """)
        pdf_display = f"""
<div class="iframe-container">
        <div class="iframe-wrapper">
            <iframe src="data:application/pdf;base64,{base64_pdf}" width="1000" height="1150" type="application/pdf"></iframe>
        </div>
</div>

"""

        # Embed PDF viewer
        # pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="700" height="900" type="application/pdf"></iframe>'

        st.markdown(pdf_display, unsafe_allow_html=True)

    pdf_url = "https://d3bd-194-164-194-154.ngrok-free.app/Angebote/" + filename

    try:
        response = requests.get(pdf_url, auth=(
            "KWE", "VV_l0cal_h0st2025"))

        if response.status_code == 200:
            pdf_data = response.content

            display_pdf(pdf_data, filename)
        else:
            st.error(
                f"Failed to fetch PDF. Status code: {response.status_code}")

    except Exception as e:
        st.error(f"An error occurred: {e}")


def grab_auftrag_and_make_dict(kunden_daten, full_awnser_list_S):
    '''
    Formatiert die Antworten für SQL
    '''
    result_dict = {
        "Vorname": st.session_state.current_Vorname,
        "Nachname": st.session_state.current_Nachname,
        "Email": str(st.session_state.current_Kontak_email).replace("@", "_AT_"),
        "Telefon": st.session_state.current_Telefonnummer_mobil,
        # KUNDEN DATEN
        "Leadquelle": st.session_state.K2,
        "Kontaktname": st.session_state.K1,
        "Abschlussdatum(voraussichtlich)": st.session_state.K3,
        "Startdatum": st.session_state.K4,

        # Gründe für anschafung
        "Grund für neue Telefonanlage: Erstanschaffung": st.session_state.K5,
        "Grund für neue Telefonanlage: Aktuelle Anlage defekt": st.session_state.K6,
        "Grund für neue Telefonanlage: Homeoffice-Funktionen benötigt": st.session_state.K7,
        "Grund für neue Telefonanlage: IT-Systeme integrieren": st.session_state.K8,
        "Grund für neue Telefonanlage: Alter aktuelle Anlage": st.session_state.K9,
        "Grund für neue Telefonanlage: Andere": st.session_state.K10,

        # Nutzer
        "AnzahlSeats": st.session_state.S1,
        "Anzahl Nutzer Windows-Client": st.session_state.S18,
        "Anzahl Nutzer macOS-Client": st.session_state.S19,
        "Anzahl iOS-Nutzer": st.session_state.S26,
        "Anzahl Android-Nutzer": st.session_state.S27,
        "Anzahl Sprachkanäle": st.session_state.S2,
        "Anzahl eFax": st.session_state.S37,
        "Anzahl analoge Faxgeräte": st.session_state.S38,
        "Anzahl Türöffner": st.session_state.S39,
        "Anzahl nur Hardware Telefon": st.session_state.S56,
        "Nutzung auf Computer per (APP)": st.session_state.S80,
        "Nutzung auf Computer per (Web)": st.session_state.S81,
        "Windows Softwarebereitstellung": st.session_state.S21,
        "Spezifische Laufzeit": st.session_state.Q4,
        "Laufzeit in Montaten": st.session_state.S55,
        "Wartungsvertrag": st.session_state.S5,
        "Video/Conferencing":  st.session_state.Q55,
        "Chat":  st.session_state.Q11,
        "Anzahl neue 10er Rufnummernblöcke": st.session_state.S6,
        "Flatrate DE Festnetz": st.session_state.Q18,
        "Flatrate DE Mobilfunk": st.session_state.Q19,
        "Flatrate EU Festnetz": st.session_state.Q20,
        "Flatrate EU Mobilfunk": st.session_state.Q21,
        "Portierung - Anzahl Rufnummernblöcke": st.session_state.S7,
        "Portierung - Anzahl Einzelrufnummern / MSNs": st.session_state.S8,
        "Unteranlagen":  st.session_state.Q53,
        "Benutzer-/Rollenkonzept für Administration":  st.session_state.Q8,
        "Deutscher Hersteller":  st.session_state.Q16,
        "Deutsche Rechenzentren":  st.session_state.Q15,
        "Sperren bestimmter Rufnummern (eingehend)":  st.session_state.Q46,
        "Gesprächsverschlüsselung":  st.session_state.Q23,
        "Single Sign-On (SSO)":  st.session_state.Q43,
        "Clip no Screening (CNS)":  st.session_state.Q13,
        "Softwarerollout per .msi":  st.session_state.Q45,
        "API - Einrichtung/System":  st.session_state.Q7,
        "Active Directory - Benutzersynchronisierung":  st.session_state.Q1,
        "Contact Center":  st.session_state.Q14,
        "Sprachaufzeichnung - automatisiert":  st.session_state.Q48,
        "Sprachaufzeichnung - on demand":  st.session_state.Q49,
        "Nachträgliche Analyse":  st.session_state.Q31,
        "Live-Analyse":  st.session_state.Q27,
        "Qualitätsmessung von Calls":  st.session_state.Q39,
        "Contact Center - Anzahl Nutzer": st.session_state.S58,
        "Sprachaufzeichnung - Anzahl Nutzer": st.session_state.S10,
        "Nachträgliche Analyse Nutzer": st.session_state.S57,
        "Nachträgliche Analyse Supervisoren": st.session_state.S11,
        "Live-Analyse Nutzer": st.session_state.S12,
        "Live-Analyse Supervisoren": st.session_state.S13,
        "Sprachansagen / IVRs":  st.session_state.Q47,
        "Anklopfen":  st.session_state.Q3,
        "Gruppen / Warteschlangen":  st.session_state.Q24,
        "Warteschlangen-Überlauf":  st.session_state.Q58,
        "Routingoptionen für Warteschlangen":  st.session_state.Q40,
        "Weiterleitungen aus Warteschlangen / Gruppen zu internen Zielen":  st.session_state.Q61,
        "Weiterleitungen aus Warteschlangen / Gruppen zu externen Zielen":  st.session_state.Q60,
        "Pickupgruppen aus Nebenstellen":  st.session_state.Q37,
        "Weiterleitung aus Nebenstellen":  st.session_state.Q59,
        "Rufumleitungsprofile":  st.session_state.Q41,
        "Chef-Sek-Funktion":  st.session_state.Q12,
        "Voicemail":  st.session_state.Q56,
        "Voicemail-Transkription":  st.session_state.Q57,
        "Zeitsteuerung":  st.session_state.Q62,
        "Weiterleitungen zu externen Zielen Anzahl": st.session_state.S14,
        "Anzahl Sprachdialoge/IVRs von extern erreichbar": st.session_state.S16,
        "Besetztlampenfelder":  st.session_state.Q9,
        "Hotdesking / Free Seating":  st.session_state.Q25,
        "Zentrales Telefonbuch":  st.session_state.Q63,
        "Kurzwahlziel extern":  st.session_state.Q26,
        "Anrufhistorie für eigene Anrufe":  st.session_state.Q5,
        "Anrufhistorie für alle Anrufe":  st.session_state.Q4,
        "Gegensprechfunktion / Interkom":  st.session_state.Q22,
        "Bitte nicht stören (DND)":  st.session_state.Q10,
        "Mehr als 1 Gerät pro Nebenstelle":  st.session_state.Q29,
        "Mehr als 1 Durchwahl pro Nebenstelle":  st.session_state.Q28,
        "Vermittlungsarbeitsplatz":  st.session_state.Q54,
        "Telefonkonferenzen":  st.session_state.Q51,
        "MS Teams Direct Routing":  st.session_state.Q30,
        "Dialer in MS Teams":  st.session_state.Q17,
        "Präsenzabgleich MS Teams":  st.session_state.Q38,
        "Anzahl Nutzer MS Teams, zusätzlich zu Nebenstelle": st.session_state.S22,
        "Anzahl Nutzer MS Teams - eigene Nebenstelle": st.session_state.S23,
        "Anzahl Dialer-Verwendung in MS Teams": st.session_state.S24,
        "Anzahl Präsenzabgleich zu MS Teams": st.session_state.S25,
        "Nebenstelle auf dem Smartphone - Android": st.session_state.Q34,
        "Nebenstelle auf dem Smartphone - iOS": st.session_state.Q35,
        "Anzahl Kleine Telefone": st.session_state.S28,
        "Kleine Telefone Optional": st.session_state.S90,
        "Anzahl Mittlere Telefone": st.session_state.S29,
        "Mittlere Telefone Optional": st.session_state.S91,
        "Anzahl Grosse Telefone": st.session_state.S30,
        "Grosse Telefone Optional": st.session_state.S92,
        "Anzahl Netzteile": st.session_state.S31,
        "Anzahl Netzteile Optional": st.session_state.S93,
        "Anzahl Expansionsmodule": st.session_state.S32,
        "Anzahl Expansionsmodule Optional": st.session_state.S94,
        "Anzahl Konferenztelefon": st.session_state.S33,
        "Anzahl Konferenztelefon Optional": st.session_state.S95,
        "Anzahl neue DECT-Mobilteile": st.session_state.S4,
        "Anzahl Singlecell - DECT - Basisstationen":  st.session_state.S9,
        "Multicell-DECT-Basisstationen":  None,
        "Rufnummernauflösung":  st.session_state.S45,
        "Click-to-Dial aus CRM/ERP":  st.session_state.S46,
        "Erfassung Anrufe in CRM/ERP":  st.session_state.S47,
        "Anruflenkung durch eingehende Nummer":  st.session_state.S48,
        "Anruflenkung durch DTMF":  st.session_state.S49,
        "Akustische Ausgabe von CRM-/ERP-Infos durch DTMF":  st.session_state.S50,
        "Integration gewünscht?":  st.session_state.S40,
        "Nebenstelle auf dem Computer - App":  st.session_state.Q32,
        "Nebenstelle auf dem Computer - webRTC":  st.session_state.Q33,
        "Slack embedded Dialer":  st.session_state.Q44,
        "TAPI 2.x":  st.session_state.Q50,
        "Zoom":  st.session_state.Q64,
        "Wird TAPI von der angegebenen Software unterstützt?":  st.session_state.S44,
        "Welche Software soll angebunden werden?":  st.session_state.S42,
        "CTI-Nutzung auf welchen Geräten?":  st.session_state.S54,
        "spezielle Art der Softwarebereitstellung":  st.session_state.S52,
        "CTI - Welches Betriebssystem ist im Einsatz?":  st.session_state.S51,
        "Anzahl Nutzer Intergration":  st.session_state.S41,
    }

    ## nicht veovia ##
    if st.session_state.current_role != "veovia":

        result_dict["Standort - Adresszeile 1"] = st.session_state["Standort - Adresszeile 1"]
        result_dict["Standort - Adresszeile 2"] = st.session_state["Standort - Adresszeile 2"]
        result_dict["Standort - Postleitzahl"] = st.session_state["Standort - Postleitzahl"]
        result_dict["Standort - Land"] = st.session_state["Standort - Land"]
        result_dict["Standort - Stadt"] = st.session_state["Standort - Stadt"]
        result_dict["partner"] = st.session_state.current_role
        # veovia
        result_dict["username"] = st.session_state.current_username
        result_dict["veovia"] = False
        result_dict["Zoho_ID"] = "No ID"

        result_dict["KundenName"] = st.session_state.KundenName
        result_dict["Datum"] = datetime.now().date()

    ## veovia ##
    if st.session_state.current_role == "veovia":
        # get Zoho ID

        get_zoho_KundenName_with_id(kunden_daten["zoho_id"])

        # nicht veovia
        result_dict["Standort - Adresszeile 1"] = st.session_state["Standort - Adresszeile 1"]
        result_dict["Standort - Adresszeile 2"] = st.session_state["Standort - Adresszeile 2"]
        result_dict["Standort - Postleitzahl"] = st.session_state["Standort - Postleitzahl"]
        result_dict["Standort - Land"] = st.session_state["Standort - Land"]
        result_dict["Standort - Stadt"] = st.session_state["Standort - Stadt"]
        result_dict["partner"] = st.session_state.current_role

        # veovia
        result_dict["username"] = st.session_state.current_username
        result_dict["veovia"] = True
        result_dict["Zoho_ID"] = st.session_state['zoho_ID']

        result_dict["KundenName"] = st.session_state.KundenName
        result_dict["Datum"] = datetime.now()

    if 'Kalkulator_id' not in st.session_state:
        Kalkulator_id = _fetch_data(
            """SELECT MAX(Kalkulator_id) AS Kalkulator_id  FROM mysql.Angebot_Anfrage """)
        _var = str(Kalkulator_id[0].get('Kalkulator_id')).replace("b'", "")
        _var2 = int(_var.replace("'", ""))
        _var2 += 1

        st.session_state.Kalkulator_id = _var2

    result_dict["Kalkulator_id"] = st.session_state.Kalkulator_id
    result_dict["Genauere Beschreibung"] = st.session_state.K11

    return result_dict


def send_dict_to_SQL(result_dict):
    '''
    Sendet die formatierten Daten an SQL 
    '''
    querry_string = "INSERT INTO mysql.Angebot_Anfrage VALUES(DEFAULT,  "
    position = 0
    max = len(result_dict.items())
    for key, value in result_dict.items():
        if not value:
            value = "False"
        if value is None:
            value = "False"
        if position != max - 1:
            querry_string += "'" + str(value) + "', "
        else:
            querry_string += "'" + str(value) + "');"
        position += 1

    _query_data(querry_string)
    id = _fetch_data("SELECT MAX(id) AS ID FROM mysql.Angebot_Anfrage ")
    return id[0].get("ID")


def chain_ID_to_User(user_id):
    _query_data(
        f"INSERT INTO mysql.user_angebot VALUES({user_id}, {st.session_state.Kalkulator_id});")


def check_database(id_to_check):
    """Helper function to check if ID exists in database"""
    try:
        db = DatabaseConnection(
            host="85.215.198.141",
            database="mysql",
            user="webapp",
            password="vv_webapp_2025",
            port="3306",
        )
        db.connect()
        querry = "SELECT * FROM mysql.Zoho_tb WHERE Zoho_ID = " + id_to_check
        result = db.execute_query(querry)

        db.disconnect()

        if not result:
            return False
        else:
            return result is not None

    except Exception as e:
        print(f"Database error: {e}")
        return False


def get_zoho_KundenName_with_id(id_to_check):
    """Helper function to check if ID exists in database"""
    try:
        db = DatabaseConnection(
            host="85.215.198.141",
            database="mysql",
            user="webapp",
            password="vv_webapp_2025",
            port="3306",
        )
        db.connect()
        querry = "SELECT * FROM mysql.Zoho_tb WHERE Zoho_ID = " + id_to_check
        result = db.execute_query(querry)

        db.disconnect()
        return result[0].get("Kunden_name")
    except Exception as e:
        print(f"Database error: {e}")
        return False


def check_id_with_webhook_fallback(id_to_check):
    """
    Checks if an ID exists in a SQL database. If not, sends a webhook request
    and retries after a 1-minute delay.

    Args:
        id_to_check (str): The ID to look up in the database
        database_path (str): Path to the SQLite database file
        webhook_url (str): URL to send the webhook request to if ID is not found

    Returns:
        dict: Result of the operation including whether ID was found and timing information
    """

    # First attempt to find the ID
    start_time = datetime.now()

    id_exists = check_database(id_to_check)

    # If ID not found, send webhook and retry after delay
    if not id_exists:
        print(f"ID {id_to_check} not found in database. Sending webhook request...")

        try:
            webhook_url = "https://hooks.zapier.com/hooks/catch/16731218/2q6r8xq/"
            # Send webhook request
            webhook_payload = {
                "id": id_to_check,
            }

            response = requests.post(webhook_url, json=webhook_payload)

            if response.status_code == 200:
                print(
                    f"Webhook sent successfully. Status code: {response.status_code}")
            else:
                print(
                    f"Webhook request failed. Status code: {response.status_code}")

        except Exception as e:
            print(f"Error sending webhook: {e}")

        # Wait for 1 minute (60 seconds)
        print("Waiting 60 seconds before retrying...")
        time.sleep(60)

        # Try again after the delay
        id_exists = check_database(id_to_check)

    end_time = datetime.now()
    time_elapsed = (end_time - start_time).total_seconds()

    return id_exists
