# Importierte Frameworks
import streamlit as st
import pandas as pd
import base64
import os
import requests
import bcrypt
from datetime import datetime
from time import sleep


# Importierte App spezifische Funktionalität
from lib.transform import get_Q_list_SQL, get_R_list_SQL, get_B_list_SQL, make_side_bar, format_for_zapier, PDF_fetcher, send_dict_to_SQL, grab_auftrag_and_make_dict, check_id_with_webhook_fallback
from lib.ProduktQ_R_B import ProduktQ_R_B
from lib.db import DatabaseConnection, fetch_data, query_data, _fetch_data, _query_data
from lib.Question import Ask_Question, Ask_Anzahl, get_group_position, ask_every_Q_with_key_name_return, ask_kunden_daten_with_key_name_return, init_Questions, get_Q_data
from lib.user import add_Angebot
from lib.admin import add_user, change_karrier_rating, change_karrier_beschreibung
from lib.log import log_User_AngebotLaden, log_User_BewertungAktualisieren, log_User_FromularLaden, log_User_FromularReset, log_User_FromularSenden, log_User_FromularSpeichern, log_User_AbrechnungenLaden, log_User_AbrechnungenLaden_ERROR, log_User_ProduktbewertungenAnderung, log_User_Produktbewertungen_initialisieren, log_ForUser, log_User_GetAllEntries
from lib.provision import add_Row_Provsion_Carrier, add_new_Provsion_Carrier, add_Row_Provsion_Partner, add_AllExisting_Provsion_Partner, search_Provsion_Carrier, search_Provsion_Partner, get_Provsion_ColumnsPartner, get_Provsion_ColumnsCarrier, get_Provsion_ListOfCarrier, get_Provsion_DataCarrier, Edit_Provsion_RowCarrier, Send_Provsion_RowCarrierZapier, get_Provsion_DataPartner, get_Provsion_ListOfPartner, Edit_Provsion_RowPartner, Send_Provsion_RowPartnerrZapier, get_Provsion_ListOfCarrierInPartner, get_Provsion_DataService, search_Provsion_Service, Edit_Provsion_RowService, Send_Provsion_RowServiceZapier
from lib.zohoAPI import APIZohoHandler
# Renderfunktionen die auf mehr als einer Seiten benutz werden


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


def get_product_status(Name):
    sql_list = fetch_data(
        f"""SELECT * FROM mysql.Produkt_tag_tb WHERE tag = '{Name}'""")

    return sql_list


def add_user_tag(user_tag):
    query_data(
        f"""INSERT INTO  mysql.produkt_tag_tb(tag, NFON_Business_Premium, NFON_Business_Standard, Avaya_Cloud_Office_Essentials, Avaya_Cloud_Office_Standard, Avaya_Cloud_Office_Premium, Avaya_Cloud_Office_Ultimate, ecotel_cloud_phone, Placetel_Profi, Digital_Phone_Business, WTG_CLOUD_PURE, Gamma_Flex_User, Gamma_Flex_Line, Eins_und_Eins_Business_Phone)
                VALUES ('{user_tag}', False, False, False, False, False, False, False, False, False, False, False, False, False);""")


def add_Partner_detials(input: list):
    querry_sting = "INSERT INTO  mysql.Partner_details VAlUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s )"
    query_data(
    )


def add_user_Abrechnungen(_username, _Partner):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    Abrechnungen_translate_dict = {
        "veovia": "veovia",
        "7Werk": "7Werk",
        "savecall": "savecall",
        "ad_hoc": "ad hoc",
        "Werth": "Werth",
        "optimaconnect": "optimaconnect",
        "ipm": "ipm",
        "top_solutions": "top solutions",
        "Schmidt_U_Fuchs": "Schmidt & Fuchs",
        "felements": "felements",
        "straight_solutions": "straight solutions",
        "Schwartz_Connect": "Schwartz Connect",
        "ac_telebusiness": "ac telebusiness",
        "ETK":  "ETK"
    }
    for keys, values in Abrechnungen_translate_dict.items():
        if _Partner == values:
            partner = keys
    id_querry = _fetch_data(
        f"SELECT id FROM mysql.Users_Tb WHERE username = '{_username}'")
    print(partner)
    respons = db.execute_query(
        f"""INSERT INTO mysql.Abrechnungen (user_id, {partner} ) VALUES ('{id_querry[0].get("id")}', '1' );""")
    print(respons)


def add_global_user_Abrechnungen(_username):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )

    id_querry = _fetch_data(
        f"SELECT id FROM mysql.Users_Tb WHERE username = '{_username}'")

    respons = db.execute_query(
        f"""INSERT INTO mysql.Abrechnungen (user_id, veovia ,  7Werk ,  savecall ,  ad_hoc ,  Werth ,  optimaconnect ,  ipm ,  top_solutions ,  Schmidt_U_Fuchs ,  felements ,  straight_solutions ,  Schwartz_Connect ,  ac_telebusiness ,  ETK ) VALUES ('{id_querry[0].get("id")}', '1', '1','1','1','1','1','1','1','1','1','1','1','1','1',);""")
    print(respons)


def update_global_user_Abrechnungen(_username):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )

    id_querry = _fetch_data(
        f"SELECT id FROM mysql.Users_Tb WHERE username = '{_username}'")

    respons = db.execute_query(
        f"""UPDATE mysql.Abrechnungen SET
        veovia = 1,
        7Werk = 1,
        savecall = 1,
        ad_hoc = 1,
        Werth = 1,
        optimaconnect = 1,
        ipm = 1,
        top_solutions = 1,
        Schmidt_U_Fuchs = 1,
        felements = 1,
        straight_solutions = 1,
        Schwartz_Connect = 1,
        ac_telebusiness = 1,
        ETK = 1 WHERE user_id = '{id_querry[0].get("id")}'
        ;""")
    print(respons)


def update_user_Abrechnungen(_username, _Partner, _input):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    Abrechnungen_translate_dict = {
        "veovia": "veovia",
        "7Werk": "7Werk",
        "savecall": "savecall",
        "ad_hoc": "ad hoc",
        "Werth": "Werth",
        "optimaconnect": "optimaconnect",
        "ipm": "ipm",
        "top_solutions": "top solutions",
        "Schmidt_U_Fuchs": "Schmidt & Fuchs",
        "felements": "felements",
        "straight_solutions": "straight solutions",
        "Schwartz_Connect": "Schwartz Connect",
        "ac_telebusiness": "ac telebusiness",
        "ETK":  "ETK"
    }
    for keys, values in Abrechnungen_translate_dict.items():
        if _Partner == values:
            partner = keys
    id_querry = db.execute_query(
        f"SELECT id FROM mysql.Users_Tb WHERE username = '{_username}'")
    if _input == True:
        _val = 1
    else:
        _val = 0
    respons = db.execute_query(
        f"""UPDATE mysql.Abrechnungen SET {partner} = {_val} WHERE user_id = {id_querry[0].get("id")};""")
    print(respons)


def get_user_list():
    _user = _fetch_data("SELECT username FROM mysql.users_tb")
    _resturn_list = []
    for entry in range(len(_user)):
        _resturn_list.append(_user[entry].get("username"))
    return _resturn_list


def get_user_Abrechnungs_status(_user_name):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )

    id_querry = db.execute_query(
        f"SELECT id FROM mysql.Users_Tb WHERE username = '{_user_name}'")

    respons = db.execute_query(
        f"""SELECT * FROM mysql.Abrechnungen WHERE user_id = {id_querry[0].get("id")}""")
    return respons


def add_Access_for_user(_username, _Accessdict: list):
    _data_user = _fetch_data(
        f"SELECT * FROM mysql.users_tb WHERE username='{_username}';")
    user_id = _data_user[0].get("id")
    querry_string = "INSERT INTO mysql.user_access VALUES ( NULl , " + \
        str(user_id) + ", " + "'" + str(_username) + "'"

    for item in range(len(_Accessdict)):
        querry_string += ", " + str(_Accessdict[item])

    querry_string += ")"

    _query_data(querry=querry_string)


def user_access_pages():
    _data_user = _fetch_data(
        f"SELECT * FROM mysql.user_access")

    return_list = []
    for keys, items in _data_user[0].items():
        if keys not in ["id", "username", "user_id"]:
            return_list.append(keys)

    return return_list


def update_Access_for_user(_username, _Accessdict: list):
    print(_Accessdict)
    pages = user_access_pages()
    print(pages)
    _data_user = _fetch_data(
        f"SELECT * FROM mysql.users_tb WHERE username='{_username}';")
    user_id = _data_user[0].get("id")
    querry_string = "UPDATE mysql.user_access SET "
    for items in range(len(pages)):
        if items == 0:
            querry_string += " " + pages[items] + "=" + \
                str(_Accessdict[items])
        else:
            querry_string += ", " + pages[items] + "=" + \
                str(_Accessdict[items])

    querry_string += f" WHERE user_id = {user_id};"
    print(querry_string)
    print(_query_data(querry_string))


def AktivInaktiveUser(_username, _bool):
    if _bool:
        _fetch_data(
            f"UPDATE mysql.users_tb SET Aktiv = '1' WHERE username = '{_username}'  ")
    if not _bool:
        _fetch_data(
            f"UPDATE mysql.users_tb SET Aktiv = '0' WHERE username = '{_username}'  ")


def test_for_Access(_username):
    _data_user = _fetch_data(
        f"SELECT * FROM mysql.user_access WHERE username='{_username}';")
    if _data_user:
        return True
    else:
        return False


def bulk_add(row):
    add_user(row.get("Username"),
             row.get("Password"),
             row.get("Partner"),
             row.get("Account email"),
             row.get("Vorname"),
             row.get("Nachname"),
             row.get("Kontakt Email"),
             row.get("Telefonnummer Mobil"),
             row.get("Telefonnummer Festnetz"),
             row.get("Zoho Nutzer ID"))

    access_list = []
    access_list.append(row.get("Angebotskonfigurator"))
    access_list.append(row.get("Angebot Verlauf"))
    access_list.append(row.get("Abrechunungen"))
    access_list.append(row.get("Produkt Bewertung"))
    access_list.append(row.get("Admin page"))
    access_list.append(row.get("Benutzer"))
    access_list.append(row.get("Abmelden"))

    add_Access_for_user(row.get("Username"), access_list)


class InFragment:
    '''
    Class to use input widgeds in Fragments
    '''
    _registry = []

    def __init__(self):
        if not any(_registry.key == self.key for _registry in InFragment._registry):
            Ask_Anzahl._registry.append(self)

    @st.fragment()
    def ask_NameOfAngebot():
        if 'SuchenKundenName' not in st.session_state:
            st.session_state.SuchenKundenName = ""

        awnser = st.text_input(
            label="Kundenname", label_visibility="hidden", placeholder="Angebot über Kundenname aufrufen", key="SuchenKundenName")
        return awnser


class FilterInFragment:
    _registry = []

    def __init__(self):
        FilterInFragment._registry.append(self)
        self.df = pd.DataFrame()

    @st.fragment()
    def ProvisionDatenFilterCarrier(self):
        with st.expander("Filter"):
            _ListeOptionen = ['Laufzeit', 'carrier', ]
            _ListeOptionen.append("Alles anzeigen")
            FilterCarrier = st.multiselect(options=_ListeOptionen,
                                           label="Durch Spalten Filter",
                                           default="Alles anzeigen",
                                           key="ProvisionDatenFilterCarrier",
                                           label_visibility="hidden"
                                           )
            if st.toggle(label="Anwenden", key="Anwenden_Carrier"):
                if not 'ListOfsearchField' in st.session_state:
                    st.session_state.ListOfsearchField = []
                st.session_state.ListOfsearchField = []

                if not 'DcitOfsearchValue' in st.session_state:
                    st.session_state.DcitOfsearchValue = {}
                st.session_state.DcitOfsearchValue = {}

                for items in FilterCarrier:
                    if "Alles anzeigen" in FilterCarrier:
                        st.session_state['Carrier_df'] = get_Provsion_DataCarrier(
                        )
                        break
                    elif items != "Alles anzeigen":
                        Search_dict = {}
                        # carrier, Laufzeit, MRR, Seat, Aktivierung, Push, Airtime
                        match items:
                            case "Laufzeit":
                                st.session_state.ListOfsearchField.append(
                                    'Laufzeit')
                                st.multiselect(options=['24 Monate', '36 Monate', '48 Monate', '60 Monate'],
                                               label=items, key="ProvisionDatenFilter_Laufzeit")
                                st.session_state.DcitOfsearchValue[
                                    'Laufzeit'] = st.session_state.ProvisionDatenFilter_Laufzeit

                            case "carrier":
                                st.session_state.ListOfsearchField.append(
                                    'carrier')
                                st.multiselect(options=get_Provsion_ListOfCarrier(),
                                               label=items, key="ProvisionDatenFilter_carrier")
                                st.session_state.DcitOfsearchValue[
                                    'carrier'] = st.session_state.ProvisionDatenFilter_carrier
                            case "MRR":
                                st.session_state.ListOfsearchField.append(
                                    'MRR')
                                st.number_input(
                                    label=items, key="ProvisionDatenFilter_MRR")
                                st.session_state.DcitOfsearchValue[
                                    'MRR'] = st.session_state.ProvisionDatenFilter_MRR
                            case "Seat":
                                st.session_state.ListOfsearchField.append(
                                    'Seat')
                                st.number_input(
                                    label=items, key="ProvisionDatenFilter_Seat")
                                st.session_state.DcitOfsearchValue[
                                    'Seat'] = st.session_state.ProvisionDatenFilter_Seat
                            case "Aktivierung":
                                st.session_state.ListOfsearchField.append(
                                    'Aktivierung')
                                st.number_input(
                                    label=items, key="ProvisionDatenFilter_Aktivierung")
                                st.session_state.DcitOfsearchValue[
                                    'Aktivierung'] = st.session_state.ProvisionDatenFilter_Aktivierung
                            case "Push":
                                st.session_state.ListOfsearchField.append(
                                    'Push')
                                st.number_input(
                                    label=items, key=f"ProvisionDatenFilter_Push")
                                st.session_state.DcitOfsearchValue[
                                    'Push'] = st.session_state.ProvisionDatenFilter_Push
                            case "Airtime":
                                st.session_state.ListOfsearchField.append(
                                    'Airtime')
                                st.number_input(
                                    label=items, key="ProvisionDatenFilter_Airtime")
                                st.session_state.DcitOfsearchValue[
                                    'Airtime'] = st.session_state.ProvisionDatenFilter_Airtime

            if st.button(label="Suchen", key="Suchen_Carrier"):
                st.session_state['Carrier_df'] = search_Provsion_Carrier(st.session_state.ListOfsearchField,
                                                                         st.session_state.DcitOfsearchValue)
                st.rerun()

    @st.fragment()
    def ProvisionDatenFilterPartner(self):
        with st.expander("Filter"):
            _ListeOptionen = ['Laufzeit', 'Partnername', 'carrier']
            _ListeOptionen.append("Alles anzeigen")
            FilterPartner = st.multiselect(options=_ListeOptionen,
                                           label="Durch Spalten Filter",
                                           default="Alles anzeigen",
                                           key="ProvisionDatenFilterPartner",
                                           label_visibility="hidden"
                                           )
            if st.toggle(label="Anwenden", key="Anwenden_Partner"):
                if not 'ListOfsearchFieldPartner' in st.session_state:
                    st.session_state.ListOfsearchFieldPartner = []
                st.session_state.ListOfsearchFieldPartner = []

                if not 'DcitOfsearchValuePartner' in st.session_state:
                    st.session_state.DcitOfsearchValuePartner = {}
                st.session_state.DcitOfsearchValuePartner = {}

                for items in FilterPartner:
                    if "Alles anzeigen" in FilterPartner:
                        st.session_state['Partner_df'] = get_Provsion_DataPartner(
                        )
                        break
                    elif items != "Alles anzeigen":
                        Search_dict = {}
                        match items:
                            case "Laufzeit":
                                st.session_state.ListOfsearchFieldPartner.append(
                                    'Laufzeit')
                                st.multiselect(options=['24 Monate', '36 Monate', '48 Monate', '60 Monate'],
                                               label=items, key="ProvisionDatenFilterPartner_Laufzeit")
                                st.session_state.DcitOfsearchValuePartner[
                                    'Laufzeit'] = st.session_state.ProvisionDatenFilterPartner_Laufzeit

                            case "carrier":
                                st.session_state.ListOfsearchFieldPartner.append(
                                    'carrier')

                                st.multiselect(options=get_Provsion_ListOfCarrierInPartner(),
                                               label=items, key="ProvisionDatenFilterPartner_carrier")

                                st.session_state.DcitOfsearchValuePartner[
                                    'carrier'] = st.session_state.ProvisionDatenFilterPartner_carrier

                            case "Partnername":
                                st.session_state.ListOfsearchFieldPartner.append(
                                    'Partnername')

                                st.multiselect(options=get_Provsion_ListOfPartner(),
                                               label=items, key="ProvisionDatenFilterPartner_partner")

                                st.session_state.DcitOfsearchValuePartner[
                                    'Partnername'] = st.session_state.ProvisionDatenFilterPartner_partner

            if st.button(label="Suchen", key="SuchenPartner"):
                st.session_state['Partner_df'] = search_Provsion_Partner(st.session_state.ListOfsearchFieldPartner,
                                                                         st.session_state.DcitOfsearchValuePartner)
                st.rerun()

    @st.fragment()
    def ProvisionDatenFilterService(self):
        with st.expander("Filter"):
            _ListeOptionen = ['Partnername']
            _ListeOptionen.append("Alles anzeigen")
            FilterService = st.multiselect(options=_ListeOptionen,
                                           label="Durch Spalten Filter",
                                           default="Alles anzeigen",
                                           key="ProvisionDatenFilterService",
                                           label_visibility="hidden"
                                           )
            if st.toggle(label="Anwenden", key="Anwenden_Service"):
                if not 'ListOfsearchFieldService' in st.session_state:
                    st.session_state.ListOfsearchFieldService = []
                st.session_state.ListOfsearchFieldService = []

                if not 'DcitOfsearchValueService' in st.session_state:
                    st.session_state.DcitOfsearchValueService = {}
                st.session_state.DcitOfsearchValueService = {}

                for items in FilterService:
                    if "Alles anzeigen" in FilterService:
                        st.session_state['Service_df'] = get_Provsion_DataService(
                        )
                        break
                    elif items != "Alles anzeigen":
                        Search_dict = {}
                        match items:
                            case "Partnername":
                                st.session_state.ListOfsearchFieldService.append(
                                    'Partnername')

                                st.multiselect(options=get_Provsion_ListOfPartner(),
                                               label=items, key="ProvisionDatenFilterService_partner")

                                st.session_state.DcitOfsearchValueService[
                                    'Partnername'] = st.session_state.ProvisionDatenFilterService_partner

            if st.button(label="Suchen", key="SuchenService"):
                st.session_state['Service_df'] = search_Provsion_Service(st.session_state.ListOfsearchFieldService,
                                                                         st.session_state.DcitOfsearchValueService)
                st.rerun()

    @st.fragment()
    def DatenEditorrCarrier(self):
        st.session_state['DataEditCarrier'] = pd.DataFrame(st.data_editor(
            st.session_state['Carrier_df'], width=705, hide_index=True))

        if st.button("Speichern", key="SpeichernCarrier"):
            dfOfChange = pd.DataFrame(st.session_state.DataEditCarrier).compare(
                st.session_state['Carrier_df'], keep_shape=True, keep_equal=True, result_names=("New", "Old"))

            for index, row in dfOfChange.iterrows():
                ChangeBool = False

                if row['id'].get('Old') != row['id'].get('New'):
                    ChangeBool = True
                if row['carrier'].get('Old') != row['carrier'].get('New'):
                    ChangeBool = True
                if row['Laufzeit'].get('Old') != row['Laufzeit'].get('New'):
                    ChangeBool = True
                if row['MRR'].get('Old') != row['MRR'].get('New'):
                    ChangeBool = True
                if row['Seat'].get('Old') != row['Seat'].get('New'):
                    ChangeBool = True
                if row['Aktivierung'].get('Old') != row['Aktivierung'].get('New'):
                    ChangeBool = True
                if row['Push'].get('Old') != row['Push'].get('New'):
                    ChangeBool = True
                if row['Airtime'].get('Old') != row['Airtime'].get('New'):
                    ChangeBool = True

                if ChangeBool == True:
                    _id = row['id'].get('Old')
                    _carrier = row['carrier'].get('Old')
                    _Laufzeit = row['Laufzeit'].get('Old')
                    _Seat = row['Seat'].get('New')
                    _mrr = row['MRR'].get('New')
                    _Aktivierung = row['Aktivierung'].get('New')
                    _Push = row['Push'].get('New')
                    _Airtime = row['Airtime'].get('New')

                    Edit_Provsion_RowCarrier(
                        _id, _carrier, _Laufzeit, _Seat, _mrr, _Aktivierung, _Push, _Airtime)
                    Send_Provsion_RowCarrierZapier(
                        _id, _carrier, _Laufzeit, _Seat, _mrr, _Aktivierung, _Push, _Airtime)
                    sleep(0.05)
            st.success("Gespeichert")

    @st.fragment()
    def DatenEditorrPartner(self):
        st.session_state['DataEditPartner'] = pd.DataFrame(st.data_editor(
            st.session_state['Partner_df'], width=705, hide_index=True))

        if st.button("Speichern", key="SpeichernPartner"):
            dfOfChange = pd.DataFrame(st.session_state.DataEditPartner).compare(
                st.session_state['Partner_df'], keep_shape=True, keep_equal=True, result_names=("New", "Old"))

            for index, row in dfOfChange.iterrows():
                ChangeBool = False

                if row['id'].get('Old') != row['id'].get('New'):
                    ChangeBool = True
                if row['carrier'].get('Old') != row['carrier'].get('New'):
                    ChangeBool = True
                if row['Laufzeit'].get('Old') != row['Laufzeit'].get('New'):
                    ChangeBool = True
                if row['MRR'].get('Old') != row['MRR'].get('New'):
                    ChangeBool = True
                if row['Seat'].get('Old') != row['Seat'].get('New'):
                    ChangeBool = True
                if row['Aktivierung'].get('Old') != row['Aktivierung'].get('New'):
                    ChangeBool = True
                if row['Push'].get('Old') != row['Push'].get('New'):
                    ChangeBool = True
                if row['Airtime'].get('Old') != row['Airtime'].get('New'):
                    ChangeBool = True

                if ChangeBool == True:
                    _id = row['id'].get('Old')
                    _carrier = row['carrier'].get('Old')
                    _Laufzeit = row['Laufzeit'].get('Old')
                    _Seat = row['Seat'].get('New')
                    _mrr = row['MRR'].get('New')
                    _Aktivierung = row['Aktivierung'].get('New')
                    _Push = row['Push'].get('New')
                    _Airtime = row['Airtime'].get('New')

                    Edit_Provsion_RowPartner(
                        _id, _carrier, _Laufzeit, _Seat, _mrr, _Aktivierung, _Push, _Airtime)

                    Send_Provsion_RowPartnerrZapier(
                        _id, _carrier, _Laufzeit, _Seat, _mrr, _Aktivierung, _Push, _Airtime)
                    sleep(0.05)
            st.success("Gespeichert")

    @st.fragment()
    def DatenEditorrService(self):
        st.session_state['DataEditService'] = pd.DataFrame(st.data_editor(
            st.session_state['Service_df'], width=705, hide_index=True))

        if st.button("Speichern", key="SpeichernService"):
            dfOfChange = pd.DataFrame(st.session_state.DataEditPartner).compare(
                st.session_state['Service_df'], keep_shape=True, keep_equal=True, result_names=("New", "Old"))

            for index, row in dfOfChange.iterrows():
                ChangeBool = False

                if row['Abschluss'].get('Old') != row['carrier'].get('New'):
                    ChangeBool = True
                if row['Airtime'].get('Old') != row['Airtime'].get('New'):
                    ChangeBool = True

                if ChangeBool == True:
                    _id = row['id'].get('Old')
                    _Abschlusse = row['Abschluss'].get('New')
                    _Airtime = row['Airtime'].get('New')

                    Edit_Provsion_RowPartner(
                        _id, _Abschlusse, _Airtime)

                    Send_Provsion_RowServiceZapier(
                        _id, _Abschlusse, _Airtime)
                    sleep(0.05)
            st.success("Gespeichert")


def Bewertung_change():
    sql_value_key = {
        "gut": "g",
        "mittel": "m",
        "schlecht": "s",
        "nicht möglich": "n"
    }

    produkt_name = st.selectbox(
        "Produkt Name", options=product_display_name())

    fragen_list = []
    for Questions in range(len(Ask_Question._registry)):
        fragen_list.append(Ask_Question._registry[Questions].name)

    frage_nummer = st.selectbox("Frage:", options=fragen_list)
    for items in range(len(ProduktQ_R_B._registry)):
        if ProduktQ_R_B._registry[items].name == produkt_name:

            for Questions in range(len(Ask_Question._registry)):
                if frage_nummer == Ask_Question._registry[Questions].name:
                    quest = format(
                        (Ask_Question._registry[Questions].key), )

            rating = ProduktQ_R_B._registry[items].R_list[quest]
            beschreibung = ProduktQ_R_B._registry[items].B_list[quest]

    st.divider()

    st.session_state.rating = rating

    bewertung = st.segmented_control(
        "Bewertung:", options=sql_value_key.keys(), default=st.session_state.rating, key="bewertung")

    beschreibung_input = st.text_input(
        "Beschreibung:", placeholder=beschreibung, value=beschreibung, key="beschreibung")

    Produkt_name_display_key = product_display_key()

    def send_Zapier_to_update(_key_name):
        url = "https://hooks.zapier.com/hooks/catch/16731218/27g4eaa/"
        db = DatabaseConnection(
            host="85.215.198.141",
            database="mysql",
            user="webapp",
            password="vv_webapp_2025",
            port="3306",
        )
        db.connect()
        data = {}
        _db = db.execute_query(
            f"""SELECT * FROM mysql.Karrier_tb WHERE Q_nmb = {_key_name} """)
        for keys, values in _db[0].items():
            data[keys] = values
        data["Q_nmb"] = _key_name

        requests.get(url, data)
    if st.button("Speichern"):
        if st.session_state.beschreibung != "" and st.session_state.bewertung != "":

            def change_bewertung(_key_name, _produkt_name, _newRating, _newBebeschreibung):
                Q_ID = _key_name
                O_ID = _key_name.replace("Q", "")
                # Change to SQL names
                Produkt = str(_produkt_name)
                Produkt = Produkt.replace(" ", "_")
                Produkt = Produkt.replace(".", "")
                Produkt = Produkt.replace("(", "")
                Produkt = Produkt.replace(")", "")
                Produkt = Produkt.replace("1&1", "eins_und_eins")
                Produkt_BE = Produkt + "_BE"
                db = DatabaseConnection(
                    host="85.215.198.141",
                    database="mysql",
                    user="webapp",
                    password="vv_webapp_2025",
                    port="3306",
                )
                db.connect()

                db.execute_query(
                    f"""UPDATE mysql.Karrier_tb SET {Produkt} = '{_newRating}' , {Produkt_BE} = '{_newBebeschreibung}' WHERE Q_nmb = '{O_ID}' """)
                log_User_ProduktbewertungenAnderung(
                    Q_ID, _produkt_name, _newRating, _newBebeschreibung)

                send_Zapier_to_update(O_ID)

            change_bewertung(quest, produkt_name,
                             sql_value_key.get(st.session_state.bewertung), st.session_state.beschreibung)

            st.write(
                "Erfolgreich geändert! Bitte Aktualisieren nachdem alle Änderung gemacht wurden")
        else:
            st.write("Beschreibung darf nicht leer sein")

    # if st.button("Komplete Tabele Speichern"):


@st.cache_resource
def product_display_name():

    sql_list = fetch_data(
        """SELECT Produkt_display_name FROM mysql.Karrier_aktive_tb""")
    name_list = []
    for items in sql_list:
        name_list.append([items][0].get('Produkt_display_name'))

    return name_list


def better_div():
    st.markdown(
        """<div class="header-container"></div>""", unsafe_allow_html=True)


def get_products_sql():
    result = _fetch_data("""SELECT * FROM mysql.Karrier_aktive_tb""")
    return result


def Get_database(id_to_check):
    """Helper function to check if ID exists in database"""
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
    kunden_Name = result[0].get("Kunden_name")
    kunden_straße = result[0].get("Straße")
    kunden_stadt = result[0].get("Stadt")
    kunden_plz = result[0].get("PLZ")
    kunden_land = result[0].get("Land")
    db.disconnect()

    return kunden_Name, kunden_straße, kunden_stadt, kunden_plz, kunden_land


def get_all_user_tags():

    sql_list = fetch_data(
        """SELECT DISTINCT tag FROM mysql.produkt_tag_tb""")

    user_tag_liste = []
    for item in sql_list:
        user_tag_liste.append([item][0]["tag"])

    return user_tag_liste


@st.cache_resource
def product_display_key():

    sql_list = fetch_data(
        """SELECT Produkt_display_name, Produkt_name FROM mysql.Karrier_aktive_tb""")
    result_dict = {}
    for items in sql_list:
        result_dict[[items][0]["Produkt_display_name"]] = [
            items][0]["Produkt_name"]

    return result_dict


def product_name_key():
    sql_list = fetch_data(
        """SELECT Produkt_display_name, Produkt_name FROM mysql.Karrier_aktive_tb""")
    result_dict = {}
    for items in sql_list:
        result_dict[[items][0]["Produkt_name"]] = [
            items][0]["Produkt_display_name"]

    return result_dict


def Produkte_Laden():
    for items in get_products_sql():
        if items['Produkt_status'] == 1:
            items['Produkt_name'] = initialize_Produkt(
                items['Produkt_display_name'], items['Produkt_name'], items['Produkt_name'] + "_BE"
            )
    return True


def get_Fragen_Name():

    result = fetch_data("""select Name, Key_Name, Gruppe FROM mysql.Fragen
    """)

    return result


def get_Gruppen_liste():

    result = fetch_data("""SELECT DISTINCT Gruppe FROM mysql.Fragen WHERE Gruppe IS NOT NULL
    """)

    return result


def change_groupe(Q_key, group):
    query_data(f"""UPDATE mysql.Fragen
                        SET Gruppe = '{group}'
                        WHERE Key_Name = '{Q_key}';""")


@st.cache_data
def initialize_Produkt(name, db_name, db_name_BE):
    return ProduktQ_R_B(
        name,
        get_Q_list_SQL(db_name),

        get_R_list_SQL(db_name),

        get_B_list_SQL(db_name_BE)
    )


# --- RENDER PAGES --- #

def render_admin():

    def get_product_status(Name):
        sql_list = fetch_data(
            f"""SELECT * FROM mysql.Produkt_tag_tb WHERE tag = '{Name}'""")

        return sql_list

    def add_user_tag(user_tag):
        query_data(
            f"""INSERT INTO  mysql.produkt_tag_tb(tag, NFON_Business_Premium, NFON_Business_Standard, Avaya_Cloud_Office_Essentials, Avaya_Cloud_Office_Standard, Avaya_Cloud_Office_Premium, Avaya_Cloud_Office_Ultimate, ecotel_cloud_phone, Placetel_Profi, Digital_Phone_Business, WTG_CLOUD_PURE, Gamma_Flex_User, Gamma_Flex_Line, Eins_und_Eins_Business_Phone)
                    VALUES ('{user_tag}', False, False, False, False, False, False, False, False, False, False, False, False, False);""")

    def add_Partner_detials(input: list):
        querry_sting = "INSERT INTO  mysql.Partner_details VAlUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s )"
        query_data(
        )

    def add_user_Abrechnungen(_username, _Partner):
        db = DatabaseConnection(
            host="85.215.198.141",
            database="mysql",
            user="webapp",
            password="vv_webapp_2025",
            port="3306",
        )
        Abrechnungen_translate_dict = {
            "veovia": "veovia",
            "7Werk": "7Werk",
            "savecall": "savecall",
            "ad_hoc": "ad hoc",
            "Werth": "Werth",
            "optimaconnect": "optimaconnect",
            "ipm": "ipm",
            "top_solutions": "top solutions",
            "Schmidt_U_Fuchs": "Schmidt & Fuchs",
            "felements": "felements",
            "straight_solutions": "straight solutions",
            "Schwartz_Connect": "Schwartz Connect",
            "ac_telebusiness": "ac telebusiness",
            "ETK":  "ETK"
        }
        for keys, values in Abrechnungen_translate_dict.items():
            if _Partner == values:
                partner = keys
        id_querry = _fetch_data(
            f"SELECT id FROM mysql.Users_Tb WHERE username = '{_username}'")
        print(partner)
        respons = db.execute_query(
            f"""INSERT INTO mysql.Abrechnungen (user_id, {partner} ) VALUES ('{id_querry[0].get("id")}', '1' );""")
        print(respons)

    def add_global_user_Abrechnungen(_username):
        db = DatabaseConnection(
            host="85.215.198.141",
            database="mysql",
            user="webapp",
            password="vv_webapp_2025",
            port="3306",
        )

        id_querry = _fetch_data(
            f"SELECT id FROM mysql.Users_Tb WHERE username = '{_username}'")

        respons = db.execute_query(
            f"""INSERT INTO mysql.Abrechnungen (user_id, veovia ,  7Werk ,  savecall ,  ad_hoc ,  Werth ,  optimaconnect ,  ipm ,  top_solutions ,  Schmidt_U_Fuchs ,  felements ,  straight_solutions ,  Schwartz_Connect ,  ac_telebusiness ,  ETK ) VALUES ('{id_querry[0].get("id")}', '1', '1','1','1','1','1','1','1','1','1','1','1','1','1',);""")
        print(respons)

    def update_global_user_Abrechnungen(_username):
        db = DatabaseConnection(
            host="85.215.198.141",
            database="mysql",
            user="webapp",
            password="vv_webapp_2025",
            port="3306",
        )

        id_querry = _fetch_data(
            f"SELECT id FROM mysql.Users_Tb WHERE username = '{_username}'")

        respons = db.execute_query(
            f"""UPDATE mysql.Abrechnungen SET
            veovia = 1,
            7Werk = 1,
            savecall = 1,
            ad_hoc = 1,
            Werth = 1,
            optimaconnect = 1,
            ipm = 1,
            top_solutions = 1,
            Schmidt_U_Fuchs = 1,
            felements = 1,
            straight_solutions = 1,
            Schwartz_Connect = 1,
            ac_telebusiness = 1,
            ETK = 1 WHERE user_id = '{id_querry[0].get("id")}'
            ;""")
        print(respons)

    def update_user_Abrechnungen(_username, _Partner, _input):
        db = DatabaseConnection(
            host="85.215.198.141",
            database="mysql",
            user="webapp",
            password="vv_webapp_2025",
            port="3306",
        )
        Abrechnungen_translate_dict = {
            "veovia": "veovia",
            "7Werk": "7Werk",
            "savecall": "savecall",
            "ad_hoc": "ad hoc",
            "Werth": "Werth",
            "optimaconnect": "optimaconnect",
            "ipm": "ipm",
            "top_solutions": "top solutions",
            "Schmidt_U_Fuchs": "Schmidt & Fuchs",
            "felements": "felements",
            "straight_solutions": "straight solutions",
            "Schwartz_Connect": "Schwartz Connect",
            "ac_telebusiness": "ac telebusiness",
            "ETK":  "ETK"
        }
        for keys, values in Abrechnungen_translate_dict.items():
            if _Partner == values:
                partner = keys
        id_querry = db.execute_query(
            f"SELECT id FROM mysql.Users_Tb WHERE username = '{_username}'")
        if _input == True:
            _val = 1
        else:
            _val = 0
        respons = db.execute_query(
            f"""UPDATE mysql.Abrechnungen SET {partner} = {_val} WHERE user_id = {id_querry[0].get("id")};""")
        print(respons)

    def get_user_list():
        _user = _fetch_data("SELECT username FROM mysql.users_tb")
        _resturn_list = []
        for entry in range(len(_user)):
            _resturn_list.append(_user[entry].get("username"))
        return _resturn_list

    def get_user_Abrechnungs_status(_user_name):
        db = DatabaseConnection(
            host="85.215.198.141",
            database="mysql",
            user="webapp",
            password="vv_webapp_2025",
            port="3306",
        )

        id_querry = db.execute_query(
            f"SELECT id FROM mysql.Users_Tb WHERE username = '{_user_name}'")

        respons = db.execute_query(
            f"""SELECT * FROM mysql.Abrechnungen WHERE user_id = {id_querry[0].get("id")}""")
        return respons

    def add_Access_for_user(_username, _Accessdict: list):
        _data_user = _fetch_data(
            f"SELECT * FROM mysql.users_tb WHERE username='{_username}';")
        user_id = _data_user[0].get("id")
        querry_string = "INSERT INTO mysql.user_access VALUES ( NULl , " + \
            str(user_id) + ", " + "'" + str(_username) + "'"

        for item in range(len(_Accessdict)):
            querry_string += ", " + str(_Accessdict[item])

        querry_string += ")"

        _query_data(querry=querry_string)

    def user_access_pages():
        _data_user = _fetch_data(
            f"SELECT * FROM mysql.user_access")

        return_list = []
        for keys, items in _data_user[0].items():
            if keys not in ["id", "username", "user_id"]:
                return_list.append(keys)

        return return_list

    def update_Access_for_user(_username, _Accessdict: list):
        print(_Accessdict)
        pages = user_access_pages()
        print(pages)
        _data_user = _fetch_data(
            f"SELECT * FROM mysql.users_tb WHERE username='{_username}';")
        user_id = _data_user[0].get("id")
        querry_string = "UPDATE mysql.user_access SET "
        for items in range(len(pages)):
            if items == 0:
                querry_string += " " + pages[items] + "=" + \
                    str(_Accessdict[items])
            else:
                querry_string += ", " + pages[items] + "=" + \
                    str(_Accessdict[items])

        querry_string += f" WHERE user_id = {user_id};"
        print(querry_string)
        print(_query_data(querry_string))

    def AktivInaktiveUser(_username, _bool):
        if _bool:
            _fetch_data(
                f"UPDATE mysql.users_tb SET Aktiv = '1' WHERE username = '{_username}'  ")
        if not _bool:
            _fetch_data(
                f"UPDATE mysql.users_tb SET Aktiv = '0' WHERE username = '{_username}'  ")

    def test_for_Access(_username):
        _data_user = _fetch_data(
            f"SELECT * FROM mysql.user_access WHERE username='{_username}';")
        if _data_user:
            return True
        else:
            return False

    def bulk_add(row):
        add_user(row.get("Username"),
                 row.get("Password"),
                 row.get("Partner"),
                 row.get("Account email"),
                 row.get("Vorname"),
                 row.get("Nachname"),
                 row.get("Kontakt Email"),
                 row.get("Telefonnummer Mobil"),
                 row.get("Telefonnummer Festnetz"),
                 row.get("Zoho Nutzer ID"))

        access_list = []
        access_list.append(row.get("Angebotskonfigurator"))
        access_list.append(row.get("Angebot Verlauf"))
        access_list.append(row.get("Abrechunungen"))
        access_list.append(row.get("Produkt Bewertung"))
        access_list.append(row.get("Admin page"))
        access_list.append(row.get("Benutzer"))
        access_list.append(row.get("Abmelden"))

        add_Access_for_user(row.get("Username"), access_list)

    row_1, row_2 = st.columns([0.1, 0.035])
    if row_1.button("Produkt Bewertung Aktualisieren"):
        st.cache_resource.clear()
        st.cache_data.clear()
        Produkte_Laden()

    if row_2.button("Daten Aktualisieren"):
        st.cache_data.clear()

    with st.expander("Neuen Partner hinzufügen "):

        new_tag = st.text_input("Name des Partners")

        if st.button("Hinzufügen"):
            add_user_tag(new_tag)

    with st.expander("Benutzer hinzufügen:"):

        def check_null_list(input: list):
            total = len(input)
            counter = 0
            for items in input:
                if items:
                    counter += 1

            if counter == total:
                return True
            else:
                return False

        if "nutzer_ID" not in st.session_state:
            st.session_state.nutzer_ID = "93065000062564001"

        st.session_state.Vorname = st.text_input(
            "Vorname", key="Neuer User Vorname")
        st.session_state.Nachname = st.text_input(
            "Nachname", key="Neuer User Nachname")
        st.session_state.username = st.text_input(
            "Username", key="Neuer User username")
        st.session_state.password = st.text_input(
            "Password", key="Neuer User password")
        st.session_state.account_Email = st.text_input(
            "Account Email", key="email_A")
        st.session_state.kontak_Email = st.text_input(
            "Kontak Email", key="email_K")
        st.session_state.Telefonnummer_Mobil = st.text_input(
            "Telefonnummer Mobil", key="tele_M")
        st.session_state.Telefonnummer_Festnetzt = st.text_input(
            "Telefonnummer Festnetz", key="tele_F")
        st.text_input(
            "Zoho Nutzer ID", key="nutzer_ID")

        role = st.selectbox("Partner", options=get_all_user_tags())

        if st.button("Speichern", key="Speichern_user"):
            input_list = [st.session_state.Vorname, st.session_state.Nachname, st.session_state.username, st.session_state.password,
                          st.session_state.account_Email, st.session_state.kontak_Email, st.session_state.Telefonnummer_Mobil, st.session_state.Telefonnummer_Festnetzt]

            result = check_null_list(input_list)
            if result == True:
                add_user(st.session_state.username, st.session_state.password, role, st.session_state.email_A, st.session_state.Vorname,
                         st.session_state.Nachname, st.session_state.email_K, st.session_state.tele_M, st.session_state.tele_F, st.session_state.nutzer_ID)

            st.write("User hinzugefügt")

    with st.expander("Batch User-Lsite hinzufügen"):
        file_excel = st.file_uploader("User Excel", ['xlsx'])
        input_amount = st.number_input("Wie viele User in Excel", value=1)
        if file_excel:
            df = pd.read_excel(file_excel)
            st.write(df)

        if st.button("Speichern", key="Speichern_bulk"):
            if file_excel:
                df = pd.read_excel(file_excel)
                if input_amount >= 1:

                    for i in range(input_amount):
                        row = df.loc[i]
                        bulk_add(row)
        #  ALL for one

    with st.expander("User für Abrechungen ändern"):
        user_liste = get_user_list()
        Abrechnungen_translate_dict = {
            "veovia": "veovia",
            "7Werk": "7Werk",
            "savecall": "savecall",
            "ad_hoc": "ad hoc",
            "Werth": "Werth",
            "optimaconnect": "optimaconnect",
            "ipm": "ipm",
            "top_solutions": "top solutions",
            "Schmidt_U_Fuchs": "Schmidt & Fuchs",
            "felements": "felements",
            "straight_solutions": "straight solutions",
            "Schwartz_Connect": "Schwartz Connect",
            "ac_telebusiness": "ac telebusiness",
            "ETK":  "ETK"
        }

        username_Abrechung = st.selectbox(options=user_liste, label="username",
                                          key="username_Abrechung_2")

        Firma_Abrechung = st.selectbox(options=Abrechnungen_translate_dict.values(), label="Firma",
                                       key="Firma_Abrechung_2")
        for keys, values in Abrechnungen_translate_dict.items():
            if values == Firma_Abrechung:
                firma_colum = keys

        try:
            _value = st.toggle("Momentaner Status", value=get_user_Abrechnungs_status(
                username_Abrechung)[0].get(firma_colum))
            if st.button("Abrechnungs Rechte ändern", key="update_single_OLD"):
                update_user_Abrechnungen(
                    username_Abrechung, Firma_Abrechung, _value)
                st.write("Hinzugefügt")
            if st.button("Globale Abrechnungs rechte Updaten"):
                update_global_user_Abrechnungen(username_Abrechung)
        except IndexError:
            st.write(
                "Dieser Benutzer hat noch keine Firma für Abrechnungne zugewiesen")
            if st.button("Abrechnungs Rechte ändern", key="update_single_NEW"):
                add_user_Abrechnungen(
                    username_Abrechung, Firma_Abrechung)
                st.write("Hinzugefügt")

    with st.expander("User Access Hinzufügen"):

        pages = user_access_pages()
        user_liste = get_user_list()
        st.selectbox(options=user_liste, label="User", key="User_Access")

        even = False
        if len(pages) % 2 == 0:
            even = True

        list_middle = int(round(((len(pages) + 1)/2), 0))

        if even == False:
            list_middle = int(round((len(pages)/2), 0))

        list_middle + 1

        left, right = st.columns(2, vertical_alignment="top")

        for item in pages[0:(list_middle)]:
            _label = str(item).replace("_", " ")
            left.checkbox(label=_label, key=item)

        for item in pages[list_middle: (len(pages)+1)]:
            _label = str(item).replace("_", " ")
            right.checkbox(label=_label, key=item)

        input_list = []
        for item in pages:
            if st.session_state[item] == True:
                input_list.append(1)
            else:
                input_list.append(0)
        _bool = st.toggle("Aktiv")

        if st.button("Speichern", key="Speichern_access"):
            if test_for_Access(st.session_state.User_Access) == False:
                add_Access_for_user(st.session_state.User_Access, input_list)
                AktivInaktiveUser(st.session_state.User_Access, _bool)
            else:
                update_Access_for_user(
                    st.session_state.User_Access, input_list)
                AktivInaktiveUser(st.session_state.User_Access, _bool)

        st.write("")

    with st.expander("Frage in Gruppe setzen:"):
        # Fragen von SQL base holen

        fragen_Data = get_Fragen_Name()
        gruppen_daten = get_Gruppen_liste()

        frage_name_list = []
        gruppen_liste = []

        for item in range(len(fragen_Data)):
            frage_name = fragen_Data[item]['Name']
            frage_name_list.append(frage_name)

        for items in range(len(gruppen_daten)):
            gruppen_liste.append(gruppen_daten[items].get('Gruppe'))

        gruppen_liste.append("Neue Gruppe")

        # select drop down

        Selection_name = st.selectbox("Frage:", options=frage_name_list)

        for item in range(len(fragen_Data)):
            if Selection_name == fragen_Data[item]['Name']:
                frage_key = fragen_Data[item]['Key_Name']
                frage_gruppe = fragen_Data[item]['Gruppe']

        st.markdown(f"Fragen ID: {frage_key}",
                    help="Wenn die ID mit S beginnt, ist die Frage für eine Anzahl, Wenn mit Q für Funktionalität")

        fragen_position = fetch_data(
            f"SELECT Fragen_P FROM mysql.Fragen WHERE Key_Name = '{frage_key}'")

        fragen_pos = fragen_position[0].get('Fragen_P')

        st.write("Momentane Position: " + format(fragen_pos, ))  #

        frage_gruppe_neu = st.segmented_control(
            "Gruppen:", options=gruppen_liste, default=frage_gruppe)

        if frage_gruppe_neu == "Neue Gruppe":
            ADD_gruppe_namen = st.text_input(
                "Wie soll die Neue Gruppe heissen?")

        fragen_liste = fetch_data(
            f"SELECT name FROM mysql.Fragen WHERE Gruppe = '{frage_gruppe_neu}' ORDER BY Fragen_P ASC")

        st.write("Fragen in dieser Gruppe:")
        for items in fragen_liste:
            name = items.get("name")
            pos_F = fetch_data(
                f"SELECT Fragen_P FROM mysql.Fragen WHERE Name = '{name}' ")
            pos_Fragen = pos_F[0].get("Fragen_P")
            st.write(format(pos_Fragen, ) + ". " + " " + name)
        st.number_input("Neue Position:", key="fragen_pos_new",
                        value=1, min_value=1, step=1)

        max_fragen = fetch_data(
            f"SELECT MAX(Fragen_P) AS Max_Fragen FROM mysql.Fragen WHERE Gruppe = '{frage_gruppe}'")
        Fragen_max = max_fragen[0].get("Max_Fragen")

        if st.button("Speichern", key="B-1000"):
            if frage_gruppe_neu != frage_gruppe:
                if frage_gruppe_neu == "Neue Gruppe":
                    change_groupe(frage_key, ADD_gruppe_namen)
                else:
                    change_groupe(frage_key, frage_gruppe_neu)
                st.write("Erfolgreich geändert!, Bitte Aktualisieren ")

            else:
                if Fragen_max == None or st.session_state.fragen_pos_new > Fragen_max:
                    query_data(
                        f"UPDATE mysql.Fragen SET Fragen_P = '{st.session_state.fragen_pos_new}' WHERE key_name = '{frage_key}'")
                else:
                    if fragen_pos < st.session_state.fragen_pos_new:
                        query_data(
                            f"UPDATE mysql.Fragen SET Fragen_P = Fragen_P - 1 WHERE Fragen_P > {fragen_pos} AND Fragen_P <= '{st.session_state.fragen_pos_new}' AND Gruppe = '{frage_gruppe}' "
                        )
                    elif fragen_pos > st.session_state.fragen_pos_new:
                        query_data(
                            f"UPDATE mysql.Fragen SET Fragen_P = Fragen_P + 1 WHERE Fragen_P >= '{st.session_state.fragen_pos_new}' AND Fragen_P < '{fragen_pos}' AND Gruppe = '{frage_gruppe}' "
                        )
                    query_data(
                        f"UPDATE mysql.Fragen SET Fragen_P = '{st.session_state.fragen_pos_new}' WHERE key_name = '{frage_key}'")

                st.write("Erfolgreich geändert!")
            st.cache_data.clear()  # dN

    with st.expander("Gruppen Reihenfolge"):

        Gruppen = get_group_position()
        Gruppen_liste = []

        for items in range(len(Gruppen)):
            if Gruppen[items].get("Gruppe") is not None:
                Gruppen_liste.append(Gruppen[items].get("Gruppe"))
                string = format(Gruppen[items].get("min_position"), ) + "." + Gruppen[items].get(
                    "Gruppe")
                st.markdown(body=string)

        st.selectbox("Welche Gruppe soll geändert werden?",
                     options=Gruppen_liste, key="group_to_change")

        st.number_input("Neue position",
                        step=1, max_value=len(Gruppen), min_value=0, key="new_position")

        if st.button("Speichern", key=201):
            def change_order(group_to_change: str, new_position: int):

                current_position = fetch_data(
                    f"SELECT max(Gruppen_p) FROM mysql.Fragen WHERE Gruppe = '{group_to_change}'; ")

                current_pos = current_position[0].get('max(Gruppen_p)')

                if current_pos is None:
                    query_data(
                        f"UPDATE mysql.Fragen SET Gruppen_p = {new_position} WHERE Gruppe = '{group_to_change}'"
                    )

                else:
                    if current_pos < new_position:
                        query_data(
                            f"UPDATE mysql.Fragen SET Gruppen_p = Gruppen_p - 1 WHERE Gruppen_p > {current_pos} AND Gruppen_p <= {new_position}"
                        )
                    elif current_pos > new_position:
                        query_data(
                            f"UPDATE mysql.Fragen SET Gruppen_p = Gruppen_p + 1 WHERE Gruppen_p >= {new_position} AND Gruppen_p < {current_pos}"
                        )
                    query_data(
                        f"UPDATE mysql.Fragen SET Gruppen_p = {new_position} WHERE Gruppe = '{group_to_change}'"
                    )

            change_order(st.session_state.group_to_change,
                         st.session_state.new_position)

    with st.expander("Produkt Deaktivieren & Aktivieren "):

        def change_produkt_status_for_user(user, produkt, new_value):

            query_data(
                f"""UPDATE mysql.produkt_tag_tb SET {produkt} = '{new_value}' WHERE tag = '{user}';""")

        def switch_produkt_status_for_user(user, produkt):

            result = fetch_data(
                f"""SELECT {produkt} FROM mysql.produkt_tag_tb WHERE tag = '{user}'; """)
            if result[0][produkt] == 1:
                query_data(
                    f"""UPDATE mysql.produkt_tag_tb SET {produkt} = '0' WHERE tag = '{user}';""")
            else:
                query_data(
                    f"""UPDATE mysql.produkt_tag_tb SET {produkt} = '1' WHERE tag = '{user}';""")

        @st.cache_resource
        def init_produkt_status(selected_user):
            produkt_status = get_product_status(selected_user)
            return produkt_status

        st.session_state.selected_user = st.selectbox(
            "Partner", options=get_all_user_tags(), key="PartnerSelection")
        produkt_status = init_produkt_status(st.session_state.selected_user)
        pruduct_liste = product_display_name()
        product_key = product_display_key()
        len_pruduct_liste = len(pruduct_liste)

        even = False
        if len_pruduct_liste % 2 == 0:
            even = True

        list_middle = int(round((len_pruduct_liste/2), 0))

        if even == False:
            list_middle = int(round((len_pruduct_liste/2), 0) + 1)

        left, right = st.columns(2, vertical_alignment="top")

        for item in pruduct_liste[0:list_middle]:
            left.checkbox(key=item, label=item,
                          value=produkt_status[0][product_key[item]])

        for item in pruduct_liste[list_middle: len_pruduct_liste]:
            right.checkbox(key=item, label=item,
                           value=produkt_status[0][product_key[item]])

        try:
            produkt_status[0].pop('tag')
        except KeyError:
            print()
        else:
            print()

        temp_dict_product = {}

        if st.button("Änderungen speichern"):
            for item in pruduct_liste:
                temp_dict_product[product_key[item]] = st.session_state[item]

            for values, keys in temp_dict_product.items():
                if temp_dict_product[values] == True:
                    change_produkt_status_for_user(
                        st.session_state.selected_user, values, 1)
                else:
                    change_produkt_status_for_user(
                        st.session_state.selected_user, values, 0)
        # selected_user_produkt_display = st.selectbox(
        #    "Produkt Prüfen", options=product_display_name())

    with st.expander("Produkt Bewertung ändern:"):
        Bewertung_change()


def render_Angebot_history():
    st.title("Angebotsverlauf:")

    def Check_custom_access():
        data = _fetch_data(
            f"SELECT custom_access FROM mysql.users_tb WHERE username = '{st.session_state.current_username}' ")
        return data[0].get("custom_access")

    def RestUserId():
        user_data = _fetch_data(
            f"SELECT id FROM mysql.users_tb WHERE username = '{st.session_state.current_username}'")
        st.session_state.user_id = user_data[0].get("id")

    def Get_custom_accessPartner():
        RestUserId()
        data = _fetch_data(
            f"SELECT distinct Access_Role FROM mysql.custom_access_tb WHERE User_id =  {st.session_state.user_id}")
        return data

    def Get_custom_accessNames(partner):
        RestUserId()
        data = _fetch_data(
            f"SELECT Access_User_FullName FROM mysql.custom_access_tb WHERE User_id =  {st.session_state.user_id} AND Access_Role = '{partner}'   ")
        return data

    def Get_custom_accessID_durchpartner(partner):
        RestUserId()
        data = _fetch_data(
            f"SELECT Access_User_id FROM mysql.custom_access_tb WHERE User_id =  {st.session_state.user_id} AND Access_Role = '{partner}' ")
        return data

    def Get_custom_accessID_durchName(name):
        RestUserId()
        data = _fetch_data(
            f"SELECT Access_User_id FROM mysql.custom_access_tb WHERE User_id =  {st.session_state.user_id} AND Access_User_FullName = '{name}' ")
        print(data)
        return data[0].get('Access_User_id')

    ##

    @st.fragment
    def AccessHandler():
        if Check_custom_access() == 1:
            data = Get_custom_accessPartner()

            partnerListe = []
            for entry in range(len(data)):
                partnerListe.append((data[entry].get('Access_Role')))

            with st.container():
                st.title("Einsicht:")

                st.selectbox(
                    "Firma", options=partnerListe, key="AccessPartner")

                data = Get_custom_accessNames(st.session_state.AccessPartner)
                namesListe = []
                for entry in range(len(data)):
                    namesListe.append(
                        (data[entry].get('Access_User_FullName')))

                st.markdown("")

                st.selectbox(
                    "Benutzer", options=namesListe, key="AccessUser")

                st.markdown("")

                st.checkbox(
                    "Alle in Benutzer in diesem Partner Anzeigen", key="UsePartnerAccess")
                st.checkbox(
                    "Nur den ausgewählten Benutzer Anzeigen ", key="UseUserAccess")

                st.markdown("")

                if st.button("Anwenden"):
                    st.rerun(scope="app")

    if "UsePartnerAccess" not in st.session_state:
        st.session_state.UsePartnerAccess = False

    if "UseUserAccess" not in st.session_state:
        st.session_state.UsePartnerAccess = False

    with st.sidebar:
        AccessHandler()

    @st.fragment
    def Eigener_Überblick():
        left, right = st.columns([0.9, 0.2], vertical_alignment="center")
        if right.button("Neu Laden", key="NeuLaden"):
            st.rerun()

        list_of_ids = _fetch_data(
            f"SELECT DISTINCT Form_ID FROM mysql.user_angebot WHERE user_id = '{st.session_state.user_id}' ORDER BY  Form_ID DESC")

        name = _fetch_data(
            f"SELECT Vorname, Nachname  FROM mysql.users_tb WHERE id = '{st.session_state.user_id}'")

        produkt_shortcut = {"NFON Business Standard": "nbs", "NFON Cloudya": "nbp",
                            "Avaya Cloud Office Essentials": "ace", "Avaya Cloud Office Standard": "acs",  "Avaya Cloud Office Premium": "acp", "Avaya Cloud Office Ultimate": "acu", "ecotel cloud.phone": "ecp", "Placetel Profi": "ptp", "Digital Phone Business": "dpb", "WTG CLOUD PURE": "wtg", "Gamma Flex (User)": "gfu", "1&1 Business Phone": "1u1", "Gamma Flex (Line)": "gfl"}
        for f_id in list_of_ids:  # LIST OF FORM ID's
            try:
                Form_ID_nmr = format(f_id.get("Form_ID"), )  # ID
                last_Ablauf = _fetch_data(
                    f"SELECT MAX(Ablauf) AS last_Ablauf FROM mysql.Angebot WHERE ID = '{Form_ID_nmr}';")[0].get('last_Ablauf')
                if last_Ablauf is not None:
                    data_for_Ablauf = _fetch_data(
                        f"SELECT * FROM mysql.Angebot WHERE Ablauf = {last_Ablauf}")
                    data_for_Title = _fetch_data(
                        f"SELECT * FROM mysql.Angebot WHERE Ablauf = {last_Ablauf} LIMIT 1")

                    kunde = decode(
                        format(data_for_Title[0].get("Kunde"), ""))
                    datum = data_for_Title[0].get("Datum")
                    time_ojb = datetime.strptime(
                        datum, "%d.%m.%Y %H:%M:%S")
                    _titel = "**" + format(kunde[2:(len(kunde)-1)]) + "**" + \
                        " " + \
                        format(time_ojb)[0:-8].replace("-", " ")
                    with st.expander(_titel):
                        if data_for_Ablauf:
                            count = 0
                            for carrier in data_for_Ablauf:
                                if carrier.get("Link") != None:
                                    if count == 0:  # title
                                        st.divider()
                                        key_ = carrier.get("Ablauf")
                                        container_ablauf = st.container(
                                            key=key_)
                                        con_title, pdf_title = container_ablauf.columns(
                                            [0.5, 0.2], border=True, vertical_alignment="center")
                                        con_title.write(
                                            format(kunde[2:(len(kunde)-1)]))
                                        nmr_mit_koma = format(
                                            round(float(carrier.get("Ablauf")), 0)).replace(".0", "")
                                        nmr_ohne_koma = nmr_mit_koma
                                        file = nmr_ohne_koma + "-" + "res" + ".pdf"

                                        text, pdf = pdf_title.columns(spec=[0.5, 0.5],
                                                                      gap="small", vertical_alignment="center")
                                        text.markdown("Vergleich:")
                                        if pdf.button("PDF Laden", key=file):
                                            PDF_fetcher(file)
                                            log_User_AngebotLaden()

                                        Produkt, Abschluss_provision, Airtime_provision, air = container_ablauf.columns([
                                            0.5, 0.3, 0.3, 0.3], vertical_alignment="bottom")

                                        Produkt.write("Produkt")
                                        Abschluss_provision.write(
                                            "Abschluss Prov.")
                                        Airtime_provision.write(
                                            "Airtime Prov.")

                                        count += 1
                                    # body

                                    Produkt, Abschluss_provision, Airtime_provision, link_pdf = container_ablauf.columns([
                                        0.5, 0.3, 0.3, 0.3], border=True, vertical_alignment="center")

                                    Produkt.write(carrier.get("Produkt"))
                                    amount_Abschluss_provision = format(carrier.get(
                                        "CP") + carrier.get("SP"), ).replace(".", ",") + " €"
                                    amount_Airtime_provision = format(carrier.get(
                                        "CA") + carrier.get("SA"), ).replace(".", ",") + " €"
                                    Abschluss_provision.write(
                                        amount_Abschluss_provision)
                                    Airtime_provision.write(
                                        format(amount_Airtime_provision), )
                                    nmr_mit_koma = format(
                                        round(float(carrier.get("Ablauf")), 0)).replace(".0", "")
                                    nmr_ohne_koma = nmr_mit_koma

                                    file = nmr_ohne_koma + "-" + \
                                        produkt_shortcut.get(
                                            carrier.get("Produkt")) + ".pdf"

                                    if link_pdf.button("PDF Laden", key=file):
                                        PDF_fetcher(file)
                                        log_User_AngebotLaden()
            except:
                print("ERROR within the id ")

    @st.fragment
    def Betreuer_Überblick(Partner):
        left, right = st.columns(
            [0.9, 0.2], vertical_alignment="center")
        if right.button("Neu Laden", key="NeuLadenBE"):
            st.rerun()

        list_of_Ma = []
        Data_of_Ma = Get_custom_accessID_durchpartner(Partner)
        for usersID in range(len(Data_of_Ma)):
            list_of_Ma.append(Data_of_Ma[usersID].get('Access_User_id'))

        for usersID in list_of_Ma:
            name = _fetch_data(
                f"SELECT Vorname, Nachname  FROM mysql.users_tb WHERE id = '{usersID}'")
            list_of_ids = _fetch_data(
                f"SELECT DISTINCT Form_ID FROM mysql.user_angebot WHERE user_id = '{usersID}' ORDER BY  Form_ID DESC")
            if list_of_ids:
                with st.container(border=True):
                    st.markdown(
                        f"{name[0].get('Vorname')} {name[0].get('Nachname')}")

                    produkt_shortcut = {"NFON Business Standard": "nbs", "NFON Cloudya": "nbp",
                                        "Avaya Cloud Office Essentials": "ace", "Avaya Cloud Office Standard": "acs",  "Avaya Cloud Office Premium": "acp", "Avaya Cloud Office Ultimate": "acu", "ecotel cloud.phone": "ecp", "Placetel Profi": "ptp", "Digital Phone Business": "dpb", "WTG CLOUD PURE": "wtg", "Gamma Flex (User)": "gfu", "1&1 Business Phone": "1u1", "Gamma Flex (Line)": "gfl"}

                    for f_id in list_of_ids:  # LIST OF FORM ID's
                        try:
                            Form_ID_nmr = format(
                                f_id.get("Form_ID"), )  # ID
                            last_Ablauf = _fetch_data(
                                f"SELECT MAX(Ablauf) AS last_Ablauf FROM mysql.Angebot WHERE ID = '{Form_ID_nmr}';")[0].get('last_Ablauf')
                            if last_Ablauf is not None:
                                data_for_Ablauf = _fetch_data(
                                    f"SELECT * FROM mysql.Angebot WHERE Ablauf = {last_Ablauf}")
                                data_for_Title = _fetch_data(
                                    f"SELECT * FROM mysql.Angebot WHERE Ablauf = {last_Ablauf} LIMIT 1")

                                kunde = decode(
                                    format(data_for_Title[0].get("Kunde"), ""))
                                datum = data_for_Title[0].get("Datum")
                                time_ojb = datetime.strptime(
                                    datum, "%d.%m.%Y %H:%M:%S")
                                _titel = "**" + format(kunde[2:(len(kunde)-1)]) + "**" + \
                                    " " + \
                                    format(time_ojb)[0:-8].replace("-", " ")
                                with st.expander(_titel):
                                    count = 0
                                    if data_for_Ablauf:
                                        for carrier in data_for_Ablauf:
                                            if carrier.get("Link") != None:
                                                if count == 0:  # title
                                                    st.divider()
                                                    key_ = carrier.get(
                                                        "Ablauf")
                                                    container_ablauf = st.container(
                                                        key=key_)
                                                    con_title, pdf_title = container_ablauf.columns(
                                                        [0.5, 0.2], border=True, vertical_alignment="center")
                                                    con_title.write(
                                                        format(kunde[2:(len(kunde)-1)]))
                                                    nmr_mit_koma = format(
                                                        round(float(carrier.get("Ablauf")), 0)).replace(".0", "")
                                                    nmr_ohne_koma = nmr_mit_koma
                                                    file = nmr_ohne_koma + "-" + "res" + ".pdf"

                                                    text, pdf = pdf_title.columns(spec=[0.5, 0.5],
                                                                                  gap="small", vertical_alignment="center")
                                                    text.markdown(
                                                        "Vergleich:")
                                                    if pdf.button("PDF Laden", key=file):
                                                        PDF_fetcher(file)
                                                        log_User_AngebotLaden()

                                                    Produkt, Abschluss_provision, Airtime_provision, air = container_ablauf.columns([
                                                        0.5, 0.3, 0.3, 0.3], vertical_alignment="bottom")

                                                    Produkt.write(
                                                        "Produkt")
                                                    Abschluss_provision.write(
                                                        "Abschluss Prov.")
                                                    Airtime_provision.write(
                                                        "Airtime Prov.")

                                                    count += 1
                                                # body
                                                Produkt, Abschluss_provision, Airtime_provision, link_pdf = container_ablauf.columns([
                                                    0.5, 0.3, 0.3, 0.3], border=True, vertical_alignment="center")

                                                Produkt.write(
                                                    carrier.get("Produkt"))
                                                amount_Abschluss_provision = format(carrier.get(
                                                    "CP") + carrier.get("SP"), ) + " €"
                                                amount_Airtime_provision = format(carrier.get(
                                                    "CA") + carrier.get("SA"), ) + " €"
                                                Abschluss_provision.write(
                                                    amount_Abschluss_provision)
                                                Airtime_provision.write(
                                                    format(amount_Airtime_provision), )
                                                nmr_mit_koma = format(
                                                    round(float(carrier.get("Ablauf")), 0)).replace(".0", "")
                                                nmr_ohne_koma = nmr_mit_koma
                                                file = nmr_ohne_koma + "-" + \
                                                    produkt_shortcut.get(
                                                        carrier.get("Produkt")) + ".pdf"

                                                if link_pdf.button("PDF Laden", key=file):
                                                    PDF_fetcher(file)
                                                    log_User_AngebotLaden()
                        except:
                            print("ERROR within the id ")

    if "UsePartnerAccess" not in st.session_state:
        st.session_state.UsePartnerAccess = False

    if "UseUserAccess" not in st.session_state:
        st.session_state.UseUserAccess = False

    if st.session_state.UseUserAccess == True:
        st.session_state.user_id = Get_custom_accessID_durchName(
            st.session_state.AccessUser)
        Eigener_Überblick()

    if st.session_state.UsePartnerAccess == True:
        Betreuer_Überblick(st.session_state.AccessPartner)

    if st.session_state.UseUserAccess == False and st.session_state.UsePartnerAccess == False:
        RestUserId()
        Eigener_Überblick()


def render_Produkt_vergleich():
    def get_from_entry(input):
        result = {}
        data = _fetch_data(
            f"SELECT * FROM mysql.Angebot_Anfrage WHERE id = {input} ")
        for key, values in data[0].items():

            old = str(values)[2:-1]
            if key == "Email":
                old = old.replace("_AT_", "@")

            if key == "KundenName":
                result['Kunden Name'] = old
            if key == "AnzahlSeats":
                result['Anzahl Seats'] = old
            result[key] = old

        return result

    def get_from_for_user():
        data = _fetch_data(
            f"""SELECT Id FROM mysql.Angebot_Anfrage WHERE username = '{st.session_state.current_username}' ORDER BY Id DESC LIMIT 5""")  # current_username
        return data

    def get_from_for_UserForClient(_KundenName):
        name = _KundenName.title()
        if "gmbh" in name:
            name = name.replace("gmbh", "GmbH")
        if "Gmbh" in name:
            name = name.replace("Gmbh", "GmbH")
            # GmbH
        data = _fetch_data(
            f"""SELECT MAX(Id) AS Id FROM mysql.Angebot_Anfrage WHERE username = '{st.session_state.current_username}' AND (KundenName LIKE UPPER('%{name}%')  OR KundenName LIKE LOWER('%{name}%') OR KundenName LIKE '%{name}%')    LIMIT 1;""")  # current_username
        return data[0].get("Id")

    def Zoho_url_to_id(input: str):
        number = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]
        return_str = ""
        for char in input:
            for numbers in number:
                if char == str(numbers):
                    return_str += char

        return return_str

    def CheckZohoIDPerAPI(_ID):
        _client_id = "1000.L2VPCGPSKTWW2K9ZOELCXGGKLYXUHH"
        _client_secret = "a67957a302cea169d48bbdee5270e5ae925b8cb0ac"
        _code = "1000.29594e025f39a146ff4ce1937cd2813e.8bcfc6b3a6b94fefd2edfc0df5241c91"

        Handler = APIZohoHandler(_client_id, _client_secret, _code)
        data, source = Handler.CheckIdForBothClientAndOpp(_ID)
        match source:
            case "Client":
                ClientData = Handler.Print_InfoClient(data)
                # [_Kundenname, _stadt, _plz, _country, _strasse, _id]
                st.session_state['zoho_ID'] = ClientData[5]
                st.session_state["Standort - Adresszeile 1"] = ClientData[4]
                st.session_state["Standort - Adresszeile 2"] = ""
                st.session_state["Standort - Postleitzahl"] = ClientData[2]
                st.session_state["Standort - Stadt"] = ClientData[1]
                st.session_state["KundenName"] = ClientData[0]
                return True
            case "Opportunity":
                ClientData = Handler.Print_InfoClient(data)
                # [_Kundenname, _stadt, _plz, _country, _strasse, _id]
                st.session_state['zoho_ID'] = ClientData[5]
                st.session_state["Standort - Adresszeile 1"] = ClientData[4]
                st.session_state["Standort - Adresszeile 2"] = ""
                st.session_state["Standort - Postleitzahl"] = ClientData[2]
                st.session_state["Standort - Stadt"] = ClientData[1]
                st.session_state["KundenName"] = ClientData[0]
                return True

            case "Not Found Anything":
                print(f"Nothing Found for {_ID} !")
                return False

    def logo_of_partner(current_role):

        if current_role == "veovia":
            st.markdown(
                "![logo](https://www.veovia.de/veo%20Logo%20BLU%20300x300.png)")
        if current_role == "7werk":
            st.markdown(
                "![logo](https://www.veovia.de/PARTNER%20INFOS/Partner%20logos/2023%207WERK%20Logo%20500px.png)")

    def clear_formular():
        for key in st.session_state.keys():
            if key.startswith("O_"):
                del st.session_state[key]
            if key.startswith("loader_"):
                del st.session_state[key]
            if key.startswith("S"):
                del st.session_state[key]
            if key.startswith("Q"):
                del st.session_state[key]
            if key.startswith("K"):
                del st.session_state[key]
            if key.startswith("check_passed"):
                st.session_state[key] = False

    def RightType(_str):
        if _str in [0, 1, 3, 4, 5, 6, 7, 8, 9]:
            return int(_str)
        if _str == "True":
            return True
        if _str == True:
            return True

        if _str == "False":
            return False
        if _str == False:
            return False
        return _str

    def set_sesstionstate(key_1, key_2, data):
        # name to key form data
        st.session_state.loader = True

        FragenKeyName = {}

        # Sonder Fragen
        for Fragen in range(len(Ask_Anzahl._registry)):
            if Ask_Anzahl._registry[Fragen].key == "KundenName":
                FragenKeyName[Ask_Anzahl._registry[Fragen].key] = "Kunden Name"

            elif Ask_Anzahl._registry[Fragen].key == "S21":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Windows Softwarebereitstellung:"
            elif Ask_Anzahl._registry[Fragen].key == "S4":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Anzahl neue DECT-Mobilteile"
            elif Ask_Anzahl._registry[Fragen].key == "S9":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Anzahl neue DECT-Mobilteile"

            elif Ask_Anzahl._registry[Fragen].key == "S23":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Anzahl Nutzer MS Teams - eigene Nebenstelle"

            elif Ask_Anzahl._registry[Fragen].key == "S26":
                FragenKeyName[Ask_Anzahl._registry[Fragen].key] = r"Anzahl iOS-Nutzer"

            elif Ask_Anzahl._registry[Fragen].key == "S11":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Nachträgliche Analyse Supervisoren"

            elif Ask_Anzahl._registry[Fragen].key == "S13":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Live-Analyse Supervisoren"

            elif Ask_Anzahl._registry[Fragen].key == "S14":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Weiterleitungen zu externen Zielen Anzahl"

            elif Ask_Anzahl._registry[Fragen].key == "S24":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Anzahl Dialer-Verwendung in MS Teams"

            elif Ask_Anzahl._registry[Fragen].key == "S28":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Anzahl Kleine Telefone"

            elif Ask_Anzahl._registry[Fragen].key == "S29":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Anzahl Mittlere Telefone"

            elif Ask_Anzahl._registry[Fragen].key == "S30":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Anzahl Grosse Telefone"
            elif Ask_Anzahl._registry[Fragen].key == "S94":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Anzahl Konferenztelefon Optional"
            elif Ask_Anzahl._registry[Fragen].key == "S41":
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = "Anzahl Nutzer Intergration"

            elif Ask_Anzahl._registry[Fragen].key in ['S90', 'S91', 'S92', 'S93', 'S94', 'S95', 'S96', 'S80', 'S81']:
                if Ask_Anzahl._registry[Fragen].key == "S90":
                    FragenKeyName[Ask_Anzahl._registry[Fragen].key] = "Kleine Telefone Optional"
                if Ask_Anzahl._registry[Fragen].key == "S91":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Mittlere Telefone Optional"
                if Ask_Anzahl._registry[Fragen].key == "S92":
                    FragenKeyName[Ask_Anzahl._registry[Fragen].key] = "Grosse Telefone Optional"
                if Ask_Anzahl._registry[Fragen].key == "S93":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Anzahl Netzteile Optional"
                if Ask_Anzahl._registry[Fragen].key == "S94":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Anzahl Expansionsmodule Optional"
                if Ask_Anzahl._registry[Fragen].key == "S94":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Anzahl Konferenztelefon Optional"
                if Ask_Anzahl._registry[Fragen].key == "S80":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Nutzung auf Computer per (APP)"
                if Ask_Anzahl._registry[Fragen].key == "S81":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Nutzung auf Computer per (Web)"

            elif Ask_Anzahl._registry[Fragen].key in ['S58', 'S57', 'S10', 'S12', 'S13']:
                if Ask_Anzahl._registry[Fragen].key == "S58":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Contact Center - Anzahl Nutzer"

                if Ask_Anzahl._registry[Fragen].key == "S57":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Nachträgliche Analyse Nutzer"

                if Ask_Anzahl._registry[Fragen].key == "S10":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Sprachaufzeichnung - Anzahl Nutzer"

                if Ask_Anzahl._registry[Fragen].key == "S12":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Nachträgliche Analyse Nutzer"

                if Ask_Anzahl._registry[Fragen].key == "S13":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Live-Analyse Nutzer"

            elif Ask_Anzahl._registry[Fragen].key in ['S14', 'S16']:
                if Ask_Anzahl._registry[Fragen].key == "S14":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Weiterleitungen zu externen Zielen Anzahl"

                if Ask_Anzahl._registry[Fragen].key == "S16":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Anzahl Sprachdialoge/IVRs von extern erreichbar"

            elif Ask_Anzahl._registry[Fragen].key in ['K5', 'K6', 'K7', 'K8', 'K9', 'K10', 'K1', 'K2', 'K11', 'K4', 'K3', "Standort - Adresszeile 1", "Standort - Adresszeile 2", "Standort - Stadt", "Standort - Land", "Standort - Postleitzahl"]:

                if Ask_Anzahl._registry[Fragen].key == "Standort - Adresszeile 1":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Standort - Adresszeile 1"
                if Ask_Anzahl._registry[Fragen].key == "Standort - Postleitzahl":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Standort - Postleitzahl"
                if Ask_Anzahl._registry[Fragen].key == "Standort - Adresszeile 2":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Standort - Adresszeile 2"
                if Ask_Anzahl._registry[Fragen].key == "Standort - Stadt":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Standort - Stadt"
                if Ask_Anzahl._registry[Fragen].key == "Standort - Land":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Standort - Land"
                if Ask_Anzahl._registry[Fragen].key == "K1":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Kontaktname"
                if Ask_Anzahl._registry[Fragen].key == "K3":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Abschlussdatum(voraussichtlich)"

                if Ask_Anzahl._registry[Fragen].key == "K4":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Startdatum"

                if Ask_Anzahl._registry[Fragen].key == "K5":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Grund für neue Telefonanlage: Erstanschaffung"
                if Ask_Anzahl._registry[Fragen].key == "K6":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Grund für neue Telefonanlage: Aktuelle Anlage defekt"
                if Ask_Anzahl._registry[Fragen].key == "K7":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Grund für neue Telefonanlage: Homeoffice-Funktionen benötigt"
                if Ask_Anzahl._registry[Fragen].key == "K8":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Grund für neue Telefonanlage: IT-Systeme integrieren"
                if Ask_Anzahl._registry[Fragen].key == "K9":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Grund für neue Telefonanlage: Alter aktuelle Anlage"
                if Ask_Anzahl._registry[Fragen].key == "K10":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Grund für neue Telefonanlage: Andere"
                if Ask_Anzahl._registry[Fragen].key == "K2":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "Leadquelle"
                if Ask_Anzahl._registry[Fragen].key == "K11":
                    FragenKeyName[Ask_Anzahl._registry[Fragen]
                                  .key] = "GenauereBeschreibung"
            else:
                FragenKeyName[Ask_Anzahl._registry[Fragen]
                              .key] = Ask_Anzahl._registry[Fragen].name

        for Fragen in range(len(Ask_Question._registry)):
            if Ask_Question._registry[Fragen].key == "Q55":
                FragenKeyName[Ask_Question._registry[Fragen].key] = r"Video & Conferencing"

            elif Ask_Question._registry[Fragen].key == "Q45":
                FragenKeyName[Ask_Question._registry[Fragen]
                              .key] = "Softwarerollout per .msi"

            elif Ask_Question._registry[Fragen].key == "Q37":
                FragenKeyName[Ask_Question._registry[Fragen]
                              .key] = "Pickupgruppen aus Nebenstellen"
            else:
                FragenKeyName[Ask_Question._registry[Fragen]
                              .key] = Ask_Question._registry[Fragen].name

        clear_formular()
        for data_name, data_values in data.items():

            for _keys, _name in FragenKeyName.items():
                # print(f"_name = {_name}")
                if data_name == _name:
                    if _keys in ['S90', 'S91', 'S92', 'S93', 'S94', 'S95', 'S96', 'S81', 'S44']:
                        if _keys == 'S44':
                            if data_values == "Optional" or data_values == "unwichtig":
                                data_values = "Nein"
                            if data_values == "wichtig":
                                data_values = "Ja"
                        if data_values == "optional":
                            data_values = True

                    _L_bool = "loader_" + _keys
                    _L = "O_" + _keys
                    st.session_state[_L] = RightType(decode(data_values))
                    st.session_state[_L_bool] = True

            if st.session_state.current_role == "veovia":
                if data_name == "Zoho_ID":
                    if data_values != None:
                        if len(data_values) == 17:
                            st.session_state.Kzoho = data_values
                            st.session_state.check_passed = True
                        else:
                            st.session_state.Kzoho = data_values
                            st.session_state.check_passed = False
            if data_name == "Kalkulator_id" and data_values is not None:
                st.session_state.Kalkulator_id = data_values

        st.rerun()

    if st.session_state.logged_in == True:
        st.markdown(
            """<style>.header-container {border-bottom: 2px solid #506e73;}</style>""", unsafe_allow_html=True)

        Produkte_Laden()
        st.title("Angebotskonfigurator:")

        C1, C2_Input, C2_Knopf, C3 = st.columns(
            [0.37, 1.5, 0.5, 0.45], vertical_alignment="bottom")
        loading_popup = C1.empty()  # Links
        loading_Search_input = C2_Input.empty()  # Links
        loading_Search_button = C2_Knopf.empty()  # Links
        loading_New = C3.empty()    # RECHTS

        #        if st.button("DEV: RELOAD"):
        #            st.cache_data.clear()
        #            st.cache_resource.clear()
        #            st.rerun()

        if "Standort - Adresszeile 1" not in st.session_state:
            st.session_state["Standort - Adresszeile 1"] = ""
        if "Standort - Adresszeile 2" not in st.session_state:
            st.session_state["Standort - Adresszeile 2"] = ""
        if "Standort - Postleitzahl" not in st.session_state:
            st.session_state["Standort - Postleitzahl"] = ""
        if "Standort - Land" not in st.session_state:
            st.session_state["Standort - Land"] = ""
        if "Standort - Stadt" not in st.session_state:
            st.session_state["Standort - Stadt"] = ""

        better_div()

        with loading_popup:
            if st.button("Neu", on_click=clear_formular):
                log_User_FromularReset()

        st.markdown("<h2 style='font-size: 30px;'>""Kunden Informationen:""</h2>",
                    unsafe_allow_html=True)

        if st.session_state.current_role == "veovia":
            with st.expander("Kunden Info (über Zoho Kundenlink Laden): "):
                if 'check_passed' not in st.session_state:
                    st.session_state.check_passed = False
                if 'empty' not in st.session_state:
                    st.session_state.empty = False

                con = st.container(height=240)
                with con:
                    zoho_ID = Zoho_url_to_id(Ask_Anzahl(key="Kzoho", name="Kunden Daten per Zoho Link laden",
                                                        help="bsp. https://crm.zoho.eu/crm/veovia/tab/Accounts/93065000072279186 ").ask_text())
                    if 'zoho_ID' not in st.session_state:
                        st.session_state.zoho_ID = 0

                    left, mid, right = con.columns(
                        [0.5, 0.5, 0.5], vertical_alignment="center")

                    if left.button("Zoho Überprüfen"):
                        if st.session_state.Kzoho != False or st.session_state.Kzoho != "False" or st.session_state.Kzoho is not None or st.session_state.Kzoho != "" or st.session_state.check_passed == False or zoho_ID != Zoho_url_to_id(st.session_state.Kzoho) or zoho_ID is not None or zoho_ID != "":
                            st.session_state.check_passed = CheckZohoIDPerAPI(
                                zoho_ID)
                            st.success("Daten geladen")

                    if st.session_state.check_passed == False and st.session_state.empty == False:
                        st.warning("Kunde / opportunity nicht gefunden")

            with st.expander("Kunden Info (manuelle eintragen):"):
                _kunden_daten = {}
                _kunden_daten["Kontaktname"] = Ask_Anzahl(
                    "K1", "Kontaktname", help="").ask_text()
                _kunden_daten['Kunden_Name'] = Ask_Anzahl(
                    key="KundenName", name="Kundenname", help="").ask_text()

                _kunden_daten['Adresszeile_1'] = Ask_Anzahl(
                    key="Standort - Adresszeile 1", name="Adresse – Zeile 1", help="").ask_text()

                _kunden_daten['Adresszeile_2'] = Ask_Anzahl(
                    key="Standort - Adresszeile 2", name="Adresse – Zeile 2", help="").ask_text()

                _kunden_daten['Postleitzahl'] = Ask_Anzahl(
                    key="Standort - Postleitzahl", name="Postleitzahl", help="").ask_text()

                _kunden_daten['Stadt'] = Ask_Anzahl(
                    key="Standort - Stadt", name="Stadt", help="").ask_text()

                _kunden_daten['Land'] = Ask_Anzahl(
                    key="Standort - Land", name="Land", help="").ask_text()

        if st.session_state.current_role != "veovia":

            with st.expander("Kunden Info:"):
                _kunden_daten = {}

                _kunden_daten["Kontaktname"] = Ask_Anzahl(
                    "K1", "Kontaktname", help="").ask_text()

                _kunden_daten['Kunden_Name'] = Ask_Anzahl(
                    key="KundenName", name="Kundenname", help="").ask_text()

                _kunden_daten['Adresszeile_1'] = Ask_Anzahl(
                    key="Standort - Adresszeile 1", name="Adresse – Zeile 1", help="").ask_text()

                _kunden_daten['Adresszeile_2'] = Ask_Anzahl(
                    key="Standort - Adresszeile 2", name="Adresse – Zeile 2", help="").ask_text()

                _kunden_daten['Postleitzahl'] = Ask_Anzahl(
                    key="Standort - Postleitzahl", name="Postleitzahl", help="").ask_text()

                _kunden_daten['Stadt'] = Ask_Anzahl(
                    key="Standort - Stadt", name="Stadt", help="").ask_text()

                _kunden_daten['Land'] = Ask_Anzahl(
                    key="Standort - Land", name="Land", help="").ask_text()

        with st.expander("Leadquelle & Abschlussdatum"):
            # Webhook

            # veovia Zoho version
            kunden_daten, kunden_daten_NameKey = ask_kunden_daten_with_key_name_return()

        if st.session_state.current_role != "veovia":
            kunden_daten.update(_kunden_daten)

        if st.session_state.current_role == "veovia":
            kunden_daten['zoho_id'] = zoho_ID
        # für alle anderen inputs

        # all inputs
        better_div()
        st.markdown("<h2 style='font-size: 30px;'>""Kundenwunsch:""</h2>",
                    unsafe_allow_html=True)

        full_awnser_list, full_awnser_list_S, Key_name_dict = ask_every_Q_with_key_name_return()
        st.html("""<style>
                    .header-Title {
                        color: black;
                        font-size: 20px;
                        display: inline;
                    }</style>
                    """)
        st.sidebar.markdown(
            """<div class="header-container"><span class="header-Title">Angebotsübersicht:</span>""", unsafe_allow_html=True)

        with st.sidebar.container(border=False):

            if st.session_state.current_role == "veovia":
                try:
                    name = _kunden_daten['Kunden_Name']
                except UnboundLocalError:
                    name = "Neuer Kunde"
                TextKundenname = f"**Kundenname**: {name}"
            else:
                if _kunden_daten['Kunden_Name'] == "" or _kunden_daten['Kunden_Name'] is None:
                    TextKundenname = f"**Kundenname**: Neuer Kunde"
                else:
                    TextKundenname = f"**Kundenname**: {_kunden_daten['Kunden_Name']}"

            TextSeats = f"**Seats**: {st.session_state.S1}"
            TextDatum = f"**Abschluss Datum**: {str(kunden_daten.get('abschluss_datum'))[:10]}"
            st.markdown(f"""{TextKundenname}  
                            {TextSeats}  
                            {TextDatum}  
                        """)

        make_side_bar(full_awnser_list, kunden_daten)

        links, rechts = st.columns([2, 0.34])
        if links.button("Absenden"):
            format_for_zapier(full_awnser_list_S, kunden_daten)
            send_dict_to_SQL(grab_auftrag_and_make_dict(
                kunden_daten, full_awnser_list_S))
            st.success("Gesendet")
            log_User_FromularSenden()

        @st.fragment
        def Find_LastEntryPerName():
            with loading_Search_input:
                Name = InFragment.ask_NameOfAngebot()

            with loading_Search_button:
                if st.button("Laden", key="SuchenKundenNameStart"):
                    try:
                        id = get_from_for_UserForClient(Name.strip())
                        id_data = get_from_entry(id)
                        set_sesstionstate(kunden_daten_NameKey,
                                          Key_name_dict, id_data)
                        log_User_FromularLaden()
                    except TypeError:
                        st.warning(
                            "Kein Angebot zu diesem Kundenname gefunden, Bitte prüfen sie Ihre Eingabe")
        Find_LastEntryPerName()

        with loading_New.popover("Kürzlich"):
            data = get_from_for_user()
            for id in range(len(data)):
                form_id = data[id].get("Id")
                for key, values in data[0].items():
                    id_data = get_from_entry(form_id)
                    with st.container(border=True):
                        left, right = st.columns([0.7, 0.4], gap="small",
                                                 vertical_alignment="center")
                        left.markdown(decode(format(
                            id_data['KundenName'], "")))
                        left.markdown(format(id_data['Datum'], "")[:16])
                        if right.button(key=form_id, label="Laden"):
                            set_sesstionstate(
                                kunden_daten_NameKey, Key_name_dict, id_data)
                            log_User_FromularLaden()

        if rechts.button("Speichern"):
            send_dict_to_SQL(grab_auftrag_and_make_dict(
                kunden_daten, full_awnser_list_S))
            st.success("Gespeichert")
            log_User_FromularSpeichern()


@st.fragment
def render_user_settings():
    st.write()
    st.title("Ihre Account Einstellungen")
    Con = st.container(height=300, border=True)
    Con.markdown("")
    Con.markdown("")
    Con.markdown(f"**Username:** {st.session_state.current_username} ")
    Con.divider()
    com_1, com_2 = Con.columns([0.5, 0.5])
    com_1.markdown(f"**Vorname:** {st.session_state.current_Vorname}")
    com_2.markdown(f"**Nachname:** {st.session_state.current_Nachname}")
    com_1.markdown(
        f"**Email Acount:** {st.session_state.current_acount_email}")
    com_2.markdown(
        f"**Email Kontakt:** {st.session_state.current_Kontak_email}")
    com_1.markdown(
        f"**Tel. mobil:** {st.session_state.current_Telefonnummer_mobil}")
    com_2.markdown(
        f"**Tel. Festnetz:** {st.session_state.current_Telefonnummer_Festnetzt}")

    with st.expander("**Passwort ändern**"):
        new_pw = st.text_input("Neues Passwort")
        new_pw_prove = st.text_input("Neues Passwort bestätingen")
        if st.button("Speichern", key="passowort"):
            if new_pw == new_pw_prove:

                bytes = new_pw.encode('utf-8')
                salt = bcrypt.gensalt()

                hash = bcrypt.hashpw(bytes, salt)

                query_data(
                    """UPDATE mysql.users_tb SET password = "%s" WHERE username = "%s" ;""" % (hash, st.session_state.current_username))
                st.write("Passwort erfolgreich geändert")

            else:
                st.write("Eingaben stimmen nicht überein")

    with st.expander("**Email & Telefonnummern ändern** "):
        with st.container(height=180):
            st.write("")
            st.text_input(
                label="Neue Kontakt Email", placeholder=st.session_state.current_Kontak_email, key="email")
            if st.button("Speichern", key="B_email_K"):
                if st.session_state.email:
                    query_data(
                        """UPDATE mysql.users_tb SET kontakt_email = "%s" WHERE username = "%s" ;""" % (st.session_state.email, st.session_state.current_username))

        with st.container(height=180):
            st.write("")
            st.text_input(
                label="Neue Account Email", placeholder=st.session_state.current_acount_email, key="email_A")
            if st.button("Speichern", key="B_email_A"):
                if st.session_state.email:
                    query_data(
                        """UPDATE mysql.users_tb SET user_email = "%s" WHERE username = "%s" ;""" % (st.session_state.email_A, st.session_state.current_username))

        with st.container(height=180):
            st.write("")
            st.text_input(
                label="Telefon Mobil ändern ", placeholder=st.session_state.current_Telefonnummer_mobil, key="mobil")
            if st.button("Speichern", key="B_mobil"):
                if st.session_state.mobil:
                    query_data(
                        """UPDATE mysql.users_tb SET Telefonnummer_mobil = "%s" WHERE username = "%s" ;""" % (st.session_state.mobil, st.session_state.current_username))

        with st.container(height=180):
            st.write("")
            st.text_input(
                label="Telefon Festnetz ändern ", placeholder=st.session_state.current_Telefonnummer_Festnetzt, key="fest")
            if st.button("Speichern", key="B_fest"):
                if st.session_state.fest:
                    query_data(
                        """UPDATE mysql.users_tb SET Telefonnummer_festnetzt = "%s" WHERE username = "%s" ;""" % (st.session_state.fest, st.session_state.current_username))


@st.fragment
def render_Abrechunungen():
    def get_user_Abrechnungs_status(_user_name):
        db = DatabaseConnection(
            host="85.215.198.141",
            database="mysql",
            user="webapp",
            password="vv_webapp_2025",
            port="3306",
        )
        Abrechnungen_translate_dict = {
            "veovia": "veovia",
            "7Werk": "7Werk",
            "savecall": "savecall",
            "ad_hoc": "ad hoc",
            "Werth": "Werth",
            "optimaconnect": "optimaconnect",
            "ipm": "ipm",
            "top_solutions": "top solutions",
            "Schmidt_U_Fuchs": "Schmidt & Fuchs",
            "felements": "felements",
            "straight_solutions": "straight solutions",
            "Schwartz_Connect": "Schwartz Connect",
            "ac_telebusiness": "ac telebusiness",
            "ETK":  "ETK"
        }

        id_querry = db.execute_query(
            f"SELECT id FROM mysql.Users_Tb WHERE username = '{_user_name}'")
        result_list = []
        respons = db.execute_query(
            f"""SELECT * FROM mysql.Abrechnungen WHERE user_id = {id_querry[0].get("id")} """)
        for keys, values in respons[0].items():
            if keys != "id" and keys != "user_id":
                if values == 1:
                    result_list.append(Abrechnungen_translate_dict.get(keys))
        return result_list

    def get_firmen_partner_details():
        '''
        Firmenname, strasse, plz, ort, primary_Farbe, secondary_Farbe, primary_font_Farbe, secondary_font_Farbe, partner_status, partner_id, zoho_nutzer_id, firmen_tag
        '''
        daten = _fetch_data(
            f"SELECT * FROM mysql.Partner_details WHERE firmen_tag = '{st.session_state.current_role}' ")

        return daten[0].get("Firmenname"), daten[0].get("strasse"), daten[0].get("plz"), daten[0].get("ort"), daten[0].get("primary_Farbe"), daten[0].get("secondary_Farbe"), daten[0].get("primary_font_Farbe"), daten[0].get("secondary_font_Farbe"), daten[0].get("partner_status"), daten[0].get("partner_id"), daten[0].get("zoho_nutzer_id"), daten[0].get("firmen_tag")

    def PDF_fetcher_Abrechung(filename):

        def display_pdf(pdf_data, filename):
            # Encode PDF to base64
            base64_pdf = base64.b64encode(pdf_data).decode('utf-8')
            # Embed PDF viewer
            pdf_display = f'''
                                <style>
                                .pdf-container {{
                                    width: 700px;
                                    height: 910px;
                                    overflow: hidden;
                                    border: none;
                                    position: relative;
                                }}
                                .pdf-container iframe {{
                                    border: none;
                                    width: 100%;
                                    height: 100%;
                                    margin: 0;
                                    padding: 0;
                                }}
                                </style>
                                <div class="pdf-container">
                                    <iframe src="data:application/pdf;base64,{base64_pdf}#toolbar=0&navpanes=0&scrollbar=0"
                                            frameborder="0"
                                            scrolling="no">
                                    </iframe>
                                </div>
                                '''
            if st.button("Vorschau", key=f"VorschauPDF{filename}"):
                st.markdown(pdf_display, unsafe_allow_html=True)

            st.download_button("PDF Herunterladen",
                               base64_pdf, file_name=filename, icon=":material/download:")

        pdf_url = "https://d3bd-194-164-194-154.ngrok-free.app/Abrechnungen/" + filename

        try:
            response = requests.get(pdf_url, auth=(
                "KWE", "VV_l0cal_h0st2025"))

            if response.status_code == 200:

                if "error exception" in str(response.content):
                    st.error("Dieses Dokument existiert nicht")
                else:
                    pdf_data = response.content
                    display_pdf(pdf_data, filename)
            else:
                st.error(
                    f"Failed to fetch PDF. Status code: {response.status_code}")

        except Exception as e:
            st.error(f"An error occurred: {e}")

    def PDF_Excel_Abrechung(filename):

        pdf_url = "https://d3bd-194-164-194-154.ngrok-free.app/Abrechnungen/" + filename

        try:
            response = requests.get(pdf_url, auth=(
                "KWE", "VV_l0cal_h0st2025"))

            if response.status_code == 200:
                excel_data = response.content
                _, right = st.columns([2, 1])
                right.download_button("Transaktions Abrechnung", data=excel_data,
                                      file_name=filename, key=filename, icon=":material/download:")
                log_User_AbrechnungenLaden()

            else:
                st.error(
                    f"Failed to fetch PDF. Status code: {response.status_code}")
                log_User_AbrechnungenLaden_ERROR()

        except Exception as e:
            st.error(f"An error occurred: {e}")

    @st.cache_data
    def PDF_fetcher_Abrechung_DataReturn(filename):
        pdf_url = "https://d3bd-194-164-194-154.ngrok-free.app/Abrechnungen/" + filename

        try:
            response = requests.get(pdf_url, auth=(
                "KWE", "VV_l0cal_h0st2025"))

            if response.status_code == 200:
                if "error exception" in str(response.content):
                    return None

                pdf_data = response.content
                base64_pdf = base64.b64encode(pdf_data).decode('utf-8')
                return pdf_data
            else:
                st.error(
                    f"Failed to fetch PDF. Status code: {response.status_code}")

        except Exception as e:
            st.error(f"An error occurred: {e}")

    @st.cache_data
    def Excel_Abrechung_DataReturn(filename):

        pdf_url = "https://d3bd-194-164-194-154.ngrok-free.app/Abrechnungen/" + filename

        try:
            response = requests.get(pdf_url, auth=(
                "KWE", "VV_l0cal_h0st2025"))

            if response.status_code == 200:
                if "error exception" in str(response.content):
                    return None
                excel_data = response.content
                return excel_data

            else:
                st.error(
                    f"Failed to fetch PDF. Status code: {response.status_code}")
                log_User_AbrechnungenLaden_ERROR()

        except Exception as e:
            st.error(f"An error occurred: {e}")

    # NGROK UPADTE SPOT

    def filename_formating(monat, jahr, partner):

        _path_pdf = r'20250120 ' + partner + \
                    r' Avaya Provisionsgutschrift ' + monat + " " + jahr + ".pdf"

        _path_excel = r'20250120 ' + partner + \
            r' Avaya Provisionsgutschrift ' + monat + " " + jahr + ".xlsx"

        return _path_pdf, _path_excel

    @st.fragment
    def make_provisionlsite(_list_jahre, _list_monate, partner):
        for Jahre in _list_jahre:
            for Monate in _list_monate:
                _path_pdf, _path_excel = filename_formating(
                    Monate, Jahre, partner)

                Name = _path_pdf.split(" ", 1)[1].replace(".pdf", "")

                base64_pdf = PDF_fetcher_Abrechung_DataReturn(
                    _path_pdf.replace(" ", "%20"))
                if base64_pdf != None:
                    st.divider()
                    con1, con2, con3, con4 = st.columns([0.9, 0.3, 0.3, 0.5])
                    excel_data = Excel_Abrechung_DataReturn(
                        _path_excel.replace(" ", "%20"))

                    con1.markdown(
                        f"Avaya Provisionsgutschrift {Monate} {Jahre}")
                    con3.download_button("PDF",
                                         base64_pdf, file_name=f"{Name}.pdf", icon=":material/download:", mime="pdf")

                    con4.download_button("Transaktionen", data=excel_data,
                                         file_name=f"{Name}.xlsx", key=_path_excel, icon=":material/download:")

                    DeCbase64_pdf = base64.b64encode(
                        base64_pdf).decode('utf-8')
                    # Embed PDF viewer
                    pdf_display = f'''
                                            <style>
                                            .pdf-container {{
                                                width: 700px;
                                                height: 910px;
                                                overflow: hidden;
                                                border: none;
                                                position: relative;
                                            }}
                                            .pdf-container iframe {{
                                                border: none;
                                                width: 100%;
                                                height: 100%;
                                                margin: 0;
                                                padding: 0;
                                            }}
                                            </style>
                                            <div class="pdf-container">
                                                <iframe src="data:application/pdf;base64,{DeCbase64_pdf}#toolbar=0&navpanes=0&scrollbar=0"
                                                        frameborder="0"
                                                        scrolling="no">
                                                </iframe>
                                            </div>
                                            '''
                    if con2.button("Vorschau", key=f"VorschauPDF{Name}"):
                        st.markdown(pdf_display, unsafe_allow_html=True)

    Abrechnungen_translate_dict = {
        "veovia": "veovia",
        "7Werk": "7Werk",
        "savecall": "savecall",
        "ad_hoc": "ad hoc",
        "Werth": "Werth",
        "optimaconnect": "optimaconnect",
        "ipm": "ipm",
        "top_solutions": "top solutions",
        "Schmidt_U_Fuchs": "Schmidt & Fuchs",
        "felements": "felements",
        "straight_solutions": "straight solutions",
        "Schwartz_Connect": "Schwartz Connect",
        "ac_telebusiness": "ac telebusiness",
        "ETK":  "ETK"
    }
    partner_acces_list = get_user_Abrechnungs_status(
        st.session_state.current_username)

    _list_monate = [
        "Dezember", "November", "Oktober",  "September",  "August", "Juli", "Juni", "Mai", "April", "März", "Februar", "Januar",
    ]
    _list_jahre = ["2025", "2024", "2023", "2022"]

    st.markdown(
        """<div class="header-container"></div>""", unsafe_allow_html=True)

    st.title(f"Abrechnungen")

    if 'partner_set' not in st.session_state:
        st.session_state.partner_set = False

    left, mid, right = st.columns([0.4, 0.4, 0.4])
    if len(partner_acces_list) > 1:
        left.selectbox("Partner", options=partner_acces_list, key="partner")
        make_provisionlsite(_list_jahre, _list_monate,
                            st.session_state.partner)

    else:
        make_provisionlsite(_list_jahre, _list_monate, partner_acces_list[0])

    # _path_pdf, _path_excel = filename_formating(
    #    st.session_state.monat, st.session_state.jahr, st.session_state.partner)

    # PDF_fetcher_Abrechung(_path_pdf.replace(" ", "%20"))
    # PDF_Excel_Abrechung(_path_excel.replace(" ", "%20"))

    # Transaktions Abrechung


@st.fragment
def render_Produkt_bewertung():

    st.title("Produktbewertung")

    if st.button("Produktbewertungen initialisieren", key="Refresh_PB"):
        log_User_Produktbewertungen_initialisieren()

        st.cache_resource.clear()
        st.cache_data.clear()
        Produkte_Laden()
        meta_question_list = get_Q_data()
        init_Questions(meta_question_list)

    Bewertung_change()


@st.fragment
def render_provision():
    st.title("Provision")

    TabProvisionCarriers, TabProvisionPartner, TabProvisionService = st.tabs(
        ['Carrier', 'Partner', 'Service'])

    with TabProvisionCarriers:

        if not 'Carrier_df' in st.session_state:
            st.session_state.Carrier_df = get_Provsion_DataCarrier()

        Carriers = FilterInFragment()
        Carriers.ProvisionDatenFilterCarrier()

        if not 'DataEditCarrier' in st.session_state:
            st.session_state.DataEditCarrier = pd.DataFrame()

        Carriers.DatenEditorrCarrier()

    with TabProvisionPartner:

        if not 'Partner_df' in st.session_state:
            st.session_state.Partner_df = get_Provsion_DataPartner()

        Partner = FilterInFragment()
        Partner.ProvisionDatenFilterPartner()

        if not 'DataEditPartner' in st.session_state:
            st.session_state.DataEditCarrier = pd.DataFrame()

        Partner.DatenEditorrPartner()

    with TabProvisionService:
        if not 'Service_df' in st.session_state:
            st.session_state.Service_df = get_Provsion_DataService()

        Service = FilterInFragment()
        Service.ProvisionDatenFilterService()

        if not 'DataEditService' in st.session_state:
            st.session_state.DataEditService = pd.DataFrame()

        Service.DatenEditorrService()


@st.fragment
def render_position_page():
    st.title("Einsichtsrechte vergeben")

    def Check_and_Give_access(username):
        data = _fetch_data(
            f"SELECT custom_access FROM mysql.users_tb WHERE username = '{username}' ")
        if data[0].get("custom_access") == 0:
            fetch_data(
                f"UPDATE mysql.users_tb SET custom_access = 1 WHERE username = '{username}'")

    def ListUserNames():
        data = _fetch_data(f"SELECT username FROM mysql.users_tb")
        returnList = []
        for entry in range(len(data)):
            returnList.append(data[entry].get('username'))
        return returnList

    def ListUserNamesPartnerSorted(Partner_Tag):
        data = _fetch_data(
            f"SELECT username FROM mysql.users_tb WHERE role = '{Partner_Tag}'")
        returnList = []
        for entry in range(len(data)):
            returnList.append(data[entry].get('username'))
        return returnList

    def ListPartnerSorted():
        data = _fetch_data(f"SELECT Distinct role FROM mysql.users_tb")
        returnList = []
        for entry in range(len(data)):
            returnList.append(data[entry].get('role'))
        return returnList

    def ListUserInPartner(partner):
        data = _fetch_data(
            f"SELECT username FROM mysql.users_tb WHERE role = '{partner}'")
        returnList = []
        for entry in range(len(data)):
            returnList.append(data[entry].get('username'))
        return returnList

    def Add_access(username, UserGetAccess):
        Check_and_Give_access(UserGetAccess)
        User = _fetch_data(
            f"SELECT id FROM mysql.users_tb WHERE username = '{UserGetAccess}'")
        data = _fetch_data(
            f"SELECT role, vorname, nachname, id FROM mysql.users_tb WHERE username = '{username}'")
        _fetch_data(
            f"INSERT INTO mysql.custom_access_tb (User_id, Access_User_id, Access_User_FullName, Access_Role) VALUES ({User[0].get('id')}, {data[0].get('id')}, '{data[0].get('vorname')} {data[0].get('nachname')}', '{data[0].get('role')}');")

    st.divider()

    st.selectbox(label="Rechte an diesen Benutzer vergeben:",
                 options=ListUserNames(), key="UserGetAccess")

    st.divider()

    Partner = st.selectbox(label="Anhand Partner Sortieren",
                           options=ListPartnerSorted(), key="SortPartner")

    if st.button("Alle Benutzer in diesem Partner Freigeben"):
        UserList = ListUserNamesPartnerSorted(Partner)
        for item in range(len(UserList)):
            Add_access(
                UserList[item], st.session_state.UserGetAccess)
        st.write("Gespeichert")
    st.markdown("**ODER**")

    st.selectbox(label="Rechte über diesen Benutzer freigeben:",
                 options=ListUserNamesPartnerSorted(st.session_state.SortPartner), key="UserToAccess")

    if st.button("Speicher"):
        Add_access(st.session_state.UserToAccess,
                   st.session_state.UserGetAccess)


@st.fragment
def render_user_admin_page():
    st.title("Nutzer hinzufügen")

    def check_null_list(input: list):
        total = len(input)
        counter = 0
        for items in input:
            if items:
                counter += 1

        if counter == total:
            return True
        else:
            return False

    if "nutzer_ID" not in st.session_state:
        st.session_state.nutzer_ID = "93065000062564001"

    st.session_state.Vorname = st.text_input(
        "Vorname", key="Neuer User Vorname")
    st.session_state.Nachname = st.text_input(
        "Nachname", key="Neuer User Nachname")
    st.session_state.username = st.text_input(
        "Username", key="Neuer User username")
    st.session_state.password = st.text_input(
        "Password", key="Neuer User password")
    st.session_state.account_Email = st.text_input(
        "Account Email", key="email_A")
    st.session_state.kontak_Email = st.text_input(
        "Kontak Email", key="email_K")
    st.session_state.Telefonnummer_Mobil = st.text_input(
        "Telefonnummer Mobil", key="tele_M")
    st.session_state.Telefonnummer_Festnetzt = st.text_input(
        "Telefonnummer Festnetz", key="tele_F")
    st.text_input(
        "Zoho Nutzer ID", key="nutzer_ID")

    role = st.selectbox("Partner", options=get_all_user_tags())

    if st.button("Speichern", key="Speichern_user"):
        input_list = [st.session_state.Vorname, st.session_state.Nachname, st.session_state.username, st.session_state.password,
                      st.session_state.account_Email, st.session_state.kontak_Email, st.session_state.Telefonnummer_Mobil, st.session_state.Telefonnummer_Festnetzt]

        result = check_null_list(input_list)
        if result == True:
            add_user(st.session_state.username, st.session_state.password, role, st.session_state.email_A, st.session_state.Vorname,
                     st.session_state.Nachname, st.session_state.email_K, st.session_state.tele_M, st.session_state.tele_F, st.session_state.nutzer_ID)

        st.write("User hinzugefügt")


@st.fragment
def render_Batch_user_admin_page():
    st.title("Mehrere Nutzer hinzufügen")

    def add_Access_for_user(_username, _Accessdict: list):
        _data_user = _fetch_data(
            f"SELECT * FROM mysql.users_tb WHERE username='{_username}';")
        user_id = _data_user[0].get("id")
        querry_string = "INSERT INTO mysql.user_access VALUES ( NULl , " + \
            str(user_id) + ", " + "'" + str(_username) + "'"

        for item in range(len(_Accessdict)):
            querry_string += ", " + str(_Accessdict[item])

        querry_string += ")"

        _query_data(querry=querry_string)

    def bulk_add(row):
        add_user(row.get("Username"),
                 row.get("Password"),
                 row.get("Partner"),
                 row.get("Account email"),
                 row.get("Vorname"),
                 row.get("Nachname"),
                 row.get("Kontakt Email"),
                 row.get("Telefonnummer Mobil"),
                 row.get("Telefonnummer Festnetz"),
                 row.get("Zoho Nutzer ID"))

        access_list = []
        access_list.append(row.get("Angebotskonfigurator"))
        access_list.append(row.get("Angebot Verlauf"))
        access_list.append(row.get("Abrechunungen"))
        access_list.append(row.get("Produkt Bewertung"))
        access_list.append(row.get("Admin page"))
        access_list.append(row.get("Benutzer"))
        access_list.append(row.get("Abmelden"))

        add_Access_for_user(row.get("Username"), access_list)

    @st.cache_data
    def convert_df(df):
        return df.to_csv().encode("utf-8")

    temp = pd.read_excel("Add_Users_tmp.xlsx")
    csv = convert_df(temp)

    st.download_button(
        label="Download Template",
        data=csv,     file_name="temp.csv",
        mime="text/csv",)

    file_excel = st.file_uploader("User Excel", ['xlsx'])
    input_amount = st.number_input("Wie viele User in Excel", value=1)
    if file_excel:
        df = pd.read_excel(file_excel)
        st.write(df)

    if st.button("Speichern", key="Speichern_bulk"):
        if file_excel:
            df = pd.read_excel(file_excel)
            if input_amount >= 1:

                for i in range(input_amount):
                    row = df.loc[i]
                    bulk_add(row)


@st.fragment
def render_Fragen_admin_page():
    st.title("Fragen verwalten")
    # Fragen von SQL base holen

    fragen_Data = get_Fragen_Name()
    gruppen_daten = get_Gruppen_liste()

    frage_name_list = []
    gruppen_liste = []

    for item in range(len(fragen_Data)):
        frage_name = fragen_Data[item]['Name']
        frage_name_list.append(frage_name)

    for items in range(len(gruppen_daten)):
        gruppen_liste.append(gruppen_daten[items].get('Gruppe'))

    gruppen_liste.append("Neue Gruppe")

    # select drop down

    Selection_name = st.selectbox("Frage:", options=frage_name_list)

    for item in range(len(fragen_Data)):
        if Selection_name == fragen_Data[item]['Name']:
            frage_key = fragen_Data[item]['Key_Name']
            frage_gruppe = fragen_Data[item]['Gruppe']

    st.markdown(f"Fragen ID: {frage_key}",
                help="Wenn die ID mit S beginnt, ist die Frage für eine Anzahl, Wenn mit Q für Funktionalität")

    fragen_position = fetch_data(
        f"SELECT Fragen_P FROM mysql.Fragen WHERE Key_Name = '{frage_key}'")

    fragen_pos = fragen_position[0].get('Fragen_P')

    st.write("Momentane Position: " + format(fragen_pos, ))  #

    frage_gruppe_neu = st.segmented_control(
        "Gruppen:", options=gruppen_liste, default=frage_gruppe)

    if frage_gruppe_neu == "Neue Gruppe":
        ADD_gruppe_namen = st.text_input(
            "Wie soll die Neue Gruppe heissen?")

    fragen_liste = fetch_data(
        f"SELECT name FROM mysql.Fragen WHERE Gruppe = '{frage_gruppe_neu}' ORDER BY Fragen_P ASC")

    st.write("Fragen in dieser Gruppe:")
    for items in fragen_liste:
        name = items.get("name")
        pos_F = fetch_data(
            f"SELECT Fragen_P FROM mysql.Fragen WHERE Name = '{name}' ")
        pos_Fragen = pos_F[0].get("Fragen_P")
        st.write(format(pos_Fragen, ) + ". " + " " + name)
    st.number_input("Neue Position:", key="fragen_pos_new",
                    value=1, min_value=1, step=1)

    max_fragen = fetch_data(
        f"SELECT MAX(Fragen_P) AS Max_Fragen FROM mysql.Fragen WHERE Gruppe = '{frage_gruppe}'")
    Fragen_max = max_fragen[0].get("Max_Fragen")

    if st.button("Speichern", key="B-1000"):
        if frage_gruppe_neu != frage_gruppe:
            if frage_gruppe_neu == "Neue Gruppe":
                change_groupe(frage_key, ADD_gruppe_namen)
            else:
                change_groupe(frage_key, frage_gruppe_neu)
            st.write("Erfolgreich geändert!, Bitte Aktualisieren ")

        else:
            if Fragen_max == None or st.session_state.fragen_pos_new > Fragen_max:
                query_data(
                    f"UPDATE mysql.Fragen SET Fragen_P = '{st.session_state.fragen_pos_new}' WHERE key_name = '{frage_key}'")
            else:
                if fragen_pos < st.session_state.fragen_pos_new:
                    query_data(
                        f"UPDATE mysql.Fragen SET Fragen_P = Fragen_P - 1 WHERE Fragen_P > {fragen_pos} AND Fragen_P <= '{st.session_state.fragen_pos_new}' AND Gruppe = '{frage_gruppe}' "
                    )
                elif fragen_pos > st.session_state.fragen_pos_new:
                    query_data(
                        f"UPDATE mysql.Fragen SET Fragen_P = Fragen_P + 1 WHERE Fragen_P >= '{st.session_state.fragen_pos_new}' AND Fragen_P < '{fragen_pos}' AND Gruppe = '{frage_gruppe}' "
                    )
                query_data(
                    f"UPDATE mysql.Fragen SET Fragen_P = '{st.session_state.fragen_pos_new}' WHERE key_name = '{frage_key}'")

            st.write("Erfolgreich geändert!")
        st.cache_data.clear()  # dN

    with st.expander("Gruppen Reihenfolge"):

        Gruppen = get_group_position()
        Gruppen_liste = []

        for items in range(len(Gruppen)):
            if Gruppen[items].get("Gruppe") is not None:
                Gruppen_liste.append(Gruppen[items].get("Gruppe"))
                string = format(Gruppen[items].get("min_position"), ) + "." + Gruppen[items].get(
                    "Gruppe")
                st.markdown(body=string)

        st.selectbox("Welche Gruppe soll geändert werden?",
                     options=Gruppen_liste, key="group_to_change")

        st.number_input("Neue position",
                        step=1, max_value=len(Gruppen), min_value=0, key="new_position")

        if st.button("Speichern", key=201):
            def change_order(group_to_change: str, new_position: int):

                current_position = fetch_data(
                    f"SELECT max(Gruppen_p) FROM mysql.Fragen WHERE Gruppe = '{group_to_change}'; ")

                current_pos = current_position[0].get('max(Gruppen_p)')

                if current_pos is None:
                    query_data(
                        f"UPDATE mysql.Fragen SET Gruppen_p = {new_position} WHERE Gruppe = '{group_to_change}'"
                    )

                else:
                    if current_pos < new_position:
                        query_data(
                            f"UPDATE mysql.Fragen SET Gruppen_p = Gruppen_p - 1 WHERE Gruppen_p > {current_pos} AND Gruppen_p <= {new_position}"
                        )
                    elif current_pos > new_position:
                        query_data(
                            f"UPDATE mysql.Fragen SET Gruppen_p = Gruppen_p + 1 WHERE Gruppen_p >= {new_position} AND Gruppen_p < {current_pos}"
                        )
                    query_data(
                        f"UPDATE mysql.Fragen SET Gruppen_p = {new_position} WHERE Gruppe = '{group_to_change}'"
                    )

            change_order(st.session_state.group_to_change,
                         st.session_state.new_position)


@st.fragment
def render_Pages_admin_page():
    st.title("Seiten- & Abrechunngen-Rechte freigeben")

    pages = user_access_pages()
    user_liste = get_user_list()
    st.selectbox(options=user_liste, label="User", key="User_Access")

    even = False
    if len(pages) % 2 == 0:
        even = True

    list_middle = int(round(((len(pages) + 1)/2), 0))

    if even == False:
        list_middle = int(round((len(pages)/2), 0))

    list_middle + 1

    left, right = st.columns(2, vertical_alignment="top")

    for item in pages[0:(list_middle)]:
        _label = str(item).replace("_", " ")
        left.checkbox(label=_label, key=item)

    for item in pages[list_middle: (len(pages)+1)]:
        _label = str(item).replace("_", " ")
        right.checkbox(label=_label, key=item)

    input_list = []
    for item in pages:
        if st.session_state[item] == True:
            input_list.append(1)
        else:
            input_list.append(0)
    _bool = st.toggle("Aktiv")

    if st.button("Speichern", key="Speichern_access"):
        if test_for_Access(st.session_state.User_Access) == False:
            add_Access_for_user(st.session_state.User_Access, input_list)
            AktivInaktiveUser(st.session_state.User_Access, _bool)
        else:
            update_Access_for_user(
                st.session_state.User_Access, input_list)
            AktivInaktiveUser(st.session_state.User_Access, _bool)
    st.divider()

    st.markdown(
        "**Wenn ein Benutzer Abrechnungen einsehen soll muss eine Firma/Partner zugewissen werden**")
    # FINDE THIS POINT
    with st.expander("User für Abrechungen ändern"):
        user_liste = get_user_list()
        Abrechnungen_translate_dict = {
            "veovia": "veovia",
            "7Werk": "7Werk",
            "savecall": "savecall",
            "ad_hoc": "ad hoc",
            "Werth": "Werth",
            "optimaconnect": "optimaconnect",
            "ipm": "ipm",
            "top_solutions": "top solutions",
            "Schmidt_U_Fuchs": "Schmidt & Fuchs",
            "felements": "felements",
            "straight_solutions": "straight solutions",
            "Schwartz_Connect": "Schwartz Connect",
            "ac_telebusiness": "ac telebusiness",
            "ETK":  "ETK"
        }

        username_Abrechung = st.selectbox(options=user_liste, label="username",
                                          key="username_Abrechung_2")

        Firma_Abrechung = st.selectbox(options=Abrechnungen_translate_dict.values(), label="Firma",
                                       key="Firma_Abrechung_2")
        for keys, values in Abrechnungen_translate_dict.items():
            if values == Firma_Abrechung:
                firma_colum = keys

        try:
            _value = st.toggle("Momentaner Status", value=get_user_Abrechnungs_status(
                username_Abrechung)[0].get(firma_colum))
            if st.button("Abrechnungs Rechte ändern", key="update_single_OLD"):
                update_user_Abrechnungen(
                    username_Abrechung, Firma_Abrechung, _value)
                st.write("Hinzugefügt")
            if st.button("Globale Abrechnungs rechte Updaten"):
                update_global_user_Abrechnungen(username_Abrechung)
        except IndexError:
            st.write(
                "Dieser Benutzer hat noch keine Firma für Abrechnungne zugewiesen")
            if st.button("Abrechnungs Rechte ändern", key="update_single_NEW"):
                add_user_Abrechnungen(
                    username_Abrechung, Firma_Abrechung)
                st.write("Hinzugefügt")


@st.fragment
def render_Produkt_admin_page():
    def get_product_status(Name):
        sql_list = fetch_data(
            f"""SELECT * FROM mysql.Produkt_tag_tb WHERE tag = '{Name}'""")

        return sql_list

    def add_user_tag(user_tag):
        query_data(
            f"""INSERT INTO  mysql.produkt_tag_tb(tag, NFON_Business_Premium, NFON_Business_Standard, Avaya_Cloud_Office_Essentials, Avaya_Cloud_Office_Standard, Avaya_Cloud_Office_Premium, Avaya_Cloud_Office_Ultimate, ecotel_cloud_phone, Placetel_Profi, Digital_Phone_Business, WTG_CLOUD_PURE, Gamma_Flex_User, Gamma_Flex_Line, Eins_und_Eins_Business_Phone)
                    VALUES ('{user_tag}', False, False, False, False, False, False, False, False, False, False, False, False, False);""")

    def add_Partner_detials(input: list):
        querry_sting = "INSERT INTO  mysql.Partner_details VAlUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s )"
        query_data(
        )

    def change_produkt_status_for_user(user, produkt, new_value):

        query_data(
            f"""UPDATE mysql.produkt_tag_tb SET {produkt} = '{new_value}' WHERE tag = '{user}';""")

    def switch_produkt_status_for_user(user, produkt):

        result = fetch_data(
            f"""SELECT {produkt} FROM mysql.produkt_tag_tb WHERE tag = '{user}'; """)
        if result[0][produkt] == 1:
            query_data(
                f"""UPDATE mysql.produkt_tag_tb SET {produkt} = '0' WHERE tag = '{user}';""")
        else:
            query_data(
                f"""UPDATE mysql.produkt_tag_tb SET {produkt} = '1' WHERE tag = '{user}';""")

    @st.cache_resource
    def init_produkt_status(selected_user):
        produkt_status = get_product_status(selected_user)
        return produkt_status

    st.session_state.selected_user = st.selectbox(
        "User Tag", options=get_all_user_tags())
    produkt_status = init_produkt_status(st.session_state.selected_user)
    pruduct_liste = product_display_name()
    product_key = product_display_key()
    len_pruduct_liste = len(pruduct_liste)

    even = False
    if len_pruduct_liste % 2 == 0:
        even = True

    list_middle = int(round((len_pruduct_liste/2), 0))

    if even == False:
        list_middle = int(round((len_pruduct_liste/2), 0) + 1)

    left, right = st.columns(2, vertical_alignment="top")

    for item in pruduct_liste[0:list_middle]:
        left.checkbox(key=item, label=item,
                      value=produkt_status[0][product_key[item]])

    for item in pruduct_liste[list_middle: len_pruduct_liste]:
        right.checkbox(key=item, label=item,
                       value=produkt_status[0][product_key[item]])

    try:
        produkt_status[0].pop('tag')
    except KeyError:
        print()
    else:
        print()

    temp_dict_product = {}

    if st.button("Änderungen speichern"):
        for item in pruduct_liste:
            temp_dict_product[product_key[item]] = st.session_state[item]

        for values, keys in temp_dict_product.items():
            if temp_dict_product[values] == True:
                change_produkt_status_for_user(
                    st.session_state.selected_user, values, 1)
            else:
                change_produkt_status_for_user(
                    st.session_state.selected_user, values, 0)
    # selected_user_produkt_display = st.selectbox(
        #    "Produkt Prüfen", options=product_display_name())


@st.fragment
def render_log_page():
    st.title("Log and Benutzer Daten")
    st.divider()

    def Fetch_UserData():
        return pd.DataFrame(fetch_data("SELECT id, username, Nachname, Vorname,  user_email, role as Partner, custom_access AS Einsichtsrecht, Aktiv,  Zoho_Nutzer_id FROM mysql.users_tb"))

    with st.expander("Benutzer Daten"):
        st.dataframe(data=Fetch_UserData(), hide_index=True)

    st.divider()

    with st.expander("gesamte Log Daten"):
        st.dataframe(data=log_User_GetAllEntries(), hide_index=True)

    st.divider()

    with st.expander("Alle Logs für UserID "):
        SearchUserID = st.text_input(
            "Nach logs nach UserId durch suchen", key="SearchUserID")

        if st.button("Suchen"):
            st.dataframe(log_ForUser(SearchUserID), hide_index=True)
