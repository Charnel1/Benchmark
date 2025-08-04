from lib.db import DatabaseConnection, fetch_data, query_data, _fetch_data, _query_data

import pandas as pd
import datetime

# Funktionen die nicht in der Benchmark zur Benutzung stehen


def batch_update_ProduktBewertung(_excelPathProduktBewertung, _LetzeZeile):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()
    df_pb = pd.read_excel(
        _excelPathProduktBewertung)

    i = 0
    while i <= _LetzeZeile:
        row = df_pb.loc[i]
        sql_querry = f"""UPDATE mysql.karrier_tb 
            SET 
            NFON_Business_Standard = '{row.get("nbs")}',
            NFON_Business_premium = '{row.get("nbp")}',
            Avaya_Cloud_Office_Essentials = '{row.get("ace")}',
            Avaya_Cloud_Office_Standard = '{row.get("acs")}',
            Avaya_Cloud_Office_Premium = '{row.get("acp")}',
            Avaya_Cloud_Office_Ultimate = '{row.get("acu")}',
            ecotel_cloud_phone = '{row.get("ecp")}',
            Placetel_Profi = '{row.get("ptp")}',
            Digital_Phone_Business = '{row.get("dpb")}',
            WTG_CLOUD_PURE = '{row.get("wtg")}',
            Gamma_Flex_User = '{row.get("gfu")}',
            eins_und_eins_Business_Phone = '{row.get("1u1")}',
            Gamma_Flex_Line = '{row.get("gfl")}',

            NFON_Business_Standard_BE = '{row.get("nbs_BE")}',
            NFON_Business_premium_BE = '{row.get("nbp_BE")}',
            Avaya_Cloud_Office_Essentials_BE = '{row.get("ace_BE")}',
            Avaya_Cloud_Office_Standard_BE = '{row.get("acs_BE")}',
            Avaya_Cloud_Office_Premium_BE = '{row.get("acp_BE")}',
            Avaya_Cloud_Office_Ultimate_BE = '{row.get("acu_BE")}',
            ecotel_cloud_phone_BE = '{row.get("ecp_BE")}',
            Placetel_Profi_BE = '{row.get("ptp_BE")}',
            Digital_Phone_Business_BE = '{row.get("dpb_BE")}',
            WTG_CLOUD_PURE_BE = '{row.get("wtg_BE")}',
            Gamma_Flex_User_BE = '{row.get("gfu_BE")}',
            eins_und_eins_Business_Phone_BE = '{row.get("1u1_BE")}',
            Gamma_Flex_Line_BE = '{row.get("gfl_BE")}'

            WHERE Q_nmb ={row.get("ID")}
            """
        db.execute_query(sql_querry)

        i += 1
    db.disconnect()


def Get_Angebot_data(row_or_All):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()

    df = pd.DataFrame(db.execute_query(
        f"SELECT {row_or_All} FROM mysql.Angebot"))

    db.disconnect()
    return df


def Get_UsernameForId(_userID):
    data = _fetch_data(
        f"SELECT username , role FROM mysql.users_tb WHERE id = {_userID}")
    print(data[0].get("username"))
    print(data[0].get("role"))
    return data[0].get("username"), data[0].get("role")


def delete_user_lsit(delete_list):
    for item in delete_list:
        print(item)
        _fetch_data(
            f"DELETE  FROM mysql.users_tb WHERE id = '{item}'   ")
        print(pd.DataFrame(_fetch_data(
            f"SELECT *  FROM mysql.users_tb WHERE id = '{item}' ")))
