from lib.db import DatabaseConnection
from lib.db import fetch_data, query_data
import datetime
import bcrypt

def add_user(_username, _password, _role, _email_A, _Vorname, _Nachname, _email_K, _tele_M, _tele_F, _nutzer_ID):

    now = datetime.datetime.now()

    bytes = _password.encode('utf-8')

    salt = bcrypt.gensalt()

    hash = bcrypt.hashpw(bytes, salt)
    query_string = """INSERT INTO mysql.users_tb (username, password, role, created_at, user_email, Vorname, Nachname, Telefonnummer_mobil, Telefonnummer_festnetzt, kontakt_email, Zoho_Nutzer_id) VALUES ('%s', "%s", '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' ); """ % (
        _username, hash, _role, now, _email_A, _Vorname, _Nachname, _tele_M, _tele_F, _email_K, _nutzer_ID)

    query_data(query_string)


def change_karrier_rating(Produkt, rating, nmb: int):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()

    db.execute_query(f"""
        UPDATE mysql.Karrier_tb
        SET {Produkt} = "{rating}"
        WHERE Q_nmb = {nmb};
                        """)
    db.disconnect()


def change_karrier_beschreibung(Produkt, beschreibung_input, nmb: int):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()
    Produkt += "_BE"
    db.execute_query(f"""
        UPDATE mysql.Karrier_tb
        SET {Produkt} = "{beschreibung_input}"
        WHERE Q_nmb = {nmb};
                        """)
    db.disconnect()
