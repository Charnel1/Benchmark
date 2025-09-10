from lib.db import DatabaseConnection, fetch_data, query_data, _fetch_data, _query_data
from lib.tools import batch_update_ProduktBewertung, Get_Angebot_data, Get_UsernameForId
from lib.log import log_User_GetAllEntries, log_User_GetEntriesForDayRange, log_User_GetEntriesForDayTimeRange, log_UniqueUser, log_ForUser
from lib.provision import add_new_Provsion_Carrier, create_ProvsionPartnerTable, create_ProvsionCarrierTable, add_Row_Provsion_Partner, add_AllExisting_Provsion_Partner, search_Provsion_Carrier, search_Provsion_Partner, Edit_Provsion_RowCarrier, Edit_Provsion_RowPartner, create_ProvsionServiceTable, add_Row_Provsion_Service
from lib.zohoAPI import APIZohoHandler

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json
import time
'''


code Link
https://accounts.zoho.com/oauth/v2/auth?scope=ZohoCRM.modules.ALL&client_id=1000.L2VPCGPSKTWW2K9ZOELCXGGKLYXUHH&response_type=code&access_type=offline&redirect_uri=https://localhost:8000
'''
# print(d)


'''fetch_data("""
CREATE TABLE mysql.PartnerProvisionZoho (
ID int NOT NULL PRIMARY KEY,
NFON_Anteil_Aktivierungsgebuhr FLOAT(5),
Avaya_Abschlussprovision_Ab_36_M_MRR FLOAT(5),
Ecotel_Abschluss_24_M FLOAT(5),
WTG_Bestand FLOAT(5),
Ring_Central_Airtimeprovision  FLOAT(5), 
Ecotel_Abschluss_48_M_MRR  FLOAT(5),
NFON_Airtimeprovision_Gesprachskosten FLOAT(5),
Ring_Central_Abschlussprovision_Ab_36_Monate_MRR FLOAT(5),
Avaya_Airtimeprovision FLOAT(5),
Ecotel_Push_60_M FLOAT(5),
Ring_Central_Abschlussprovision_Bis_35_Monate_MRR FLOAT(5),
WTG_Abschluss_60_M FLOAT(5),
Ecotel_Push_48_M FLOAT(5),
Ecotel_Push_36_M FLOAT(5),
Ecotel_Push_24_M FLOAT(5),
NFON_Airtimeprovision_Nebenstelle  FLOAT(5),
WTG_Airtimeprovision FLOAT(5),
Ecotel_Abschluss_36_M_MRR FLOAT(5),
Ecotel_Airtimeprovision FLOAT(5),
WTG_Neu_Abschluss_36_M FLOAT(5),
Ecotel_Abschluss_60_M_MRR FLOAT(5),
WTG_Abschluss_48_M FLOAT(5),
Avaya_Abschlussprovision_Bis_35_Monate_MRR FLOAT(5),
WTG_Neu_Abschluss_24_M FLOAT(5),
Provision_Nachlass FLOAT(5)

_query_data("""
            CREATE TABLE mysql.Betreuer_Access (
            ID int NOT NULL PRIMARY KEY,
            User_id INT,
            Tag_Partner Varchar(255));   """)

            
ALTER TABLE table_name ADD column_name datatype DEFAULT 'Sandnes';

UPDATE table_name
SET column1 = value1, column2 = value2, ...
WHERE condition;

Vorname, Nachname


users_tb:

    Betreuer_Status
    position

""")

INSERT INTO table_name (column1, column2, column3, ...)
VALUES (value1, value2, value3, ...);

16 
ALTER TABLE table_name
MODIFY column_name datatype;
 
'''


# int((fetch_data("""
# ALTER TABLE mysql.users_tb ADD position Char(2) DEFAULT 'MA';
# """)))ALTER TABLE table_name
# DROP COLUMN column_name


# print((fetch_data(""" UPDATE mysql.users_tb SET custom_access = 0 WHERE username = 'RonaldBals'   """)))

# position_page
# print(pd.DataFrame(

# 56070 Koblenz Tel.: +49261 20 16 77 0 Fax: +49261 20 16 77 50 E-Mail: info@4service-systems.de Geschäftsführung Stefan Mallmann


# ))

# print(_fetch_data(
# f"SELECT DISTINCT Form_ID FROM mysql.user_angebot WHERE role = 'veovia' AND position = 'MA' ORDER BY  Form_ID DESC"))

#


# _fetch_data(
#    f"DELETE  FROM mysql.users_tb WHERE id = {id}   ")

#


def delete_user_lsit(delete_list):
    for item in delete_list:
        print(pd.DataFrame(_fetch_data(
            f"SELECT *  FROM mysql.users_tb WHERE id = '{item}' ")))
        _fetch_data(
            f"DELETE  FROM mysql.users_tb WHERE id = '{item}'   ")
        print(pd.DataFrame(_fetch_data(
            f"SELECT *  FROM mysql.users_tb WHERE id = '{item}' ")))


# delete_list = [34, 18, 14, 37, 19, 26, 59]

# position_page rename

#


# delete_user_lsit(delete_list)

# log_ForUser()
# nachname
"""Partnername lang: AS-Systeme IT Business GmbH
Partnername kurz: AS-Systeme

Adresse:  
AS-Systeme GmbH
Wankelstrasse 1
70563 Stuttgart


User: erstmal Moritz Regmann

Farben 
Primärfarbe RGB(175, 39, 47)
Sekundärfarbe RGB(99,113,134)
Primärschriftfarbe RGB(252,252,252)
Sekundärschriftfarbe RGB(252,252,252)"""
# ADMIN PAGES IN MYSQL.USER_ACCESS:
# user_admin
# Batch_user_admin
# Fragen_admin
# Pages_admin
# Produkt_admin

# print(pd.DataFrame(_fetch_data(
# print(pd.DataFrame(_fetch_data(
#    "SELECT * FROM mysql.users_tb WHERE username = 'FelixKohler'")))
#    "Update mysql.users_tb SET nachname = 'Köhler', Aktiv = '1' WHERE username = 'FelixKohler'")))
# 93065000064156205


_query_data("INSERT INTO mysql.partner_details  VALUES ('AS-Systeme IT Business GmbH', 'Wankelstrasse 1', '70563', 'Stuttgart', 'RGB(175, 39, 47)', 'RGB(99,113,134)',  'RGB(252,252,252)', 'RGB(252,252,252)', 'Professional-Partner', 16, '93065000064156205', 'AS-Systeme');")
