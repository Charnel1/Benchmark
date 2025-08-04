import hashlib
import streamlit as st
from lib.db import DatabaseConnection, fetch_data, query_data

import requests
import bcrypt
import random
import string
import datetime


def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def add_Angebot(full_awnser_list, full_awnser_list_S, kunden_daten):
    fetch_data(f"""   """)


def reset_passwort(user_to_reset):
    try:
        user_to_reset_data = fetch_data(
            """SELECT * FROM mysql.users_tb WHERE username = "%s" ;""" % (user_to_reset))
        # over write with random pw adn send per email
        # url =
        new_pw = ""
        length = 12
        for i in range(length):
            new_pw = new_pw + random.choice(string.ascii_letters)
        # end for

        data = {
            "username": user_to_reset,
            "email": user_to_reset_data[0].get("user_email"),
            "Neues Passwort": new_pw}
        url = "https://hooks.zapier.com/hooks/catch/16731218/2lyo8lq/"

        requests.post(
            url,
            data)

        bytes = new_pw.encode('utf-8')
        salt = bcrypt.gensalt()

        hash = bcrypt.hashpw(bytes, salt)
        fetch_data(
            """UPDATE mysql.users_tb SET password = "%s" WHERE username = "%s" ;""" % (hash, user_to_reset))
        st.success(
            "Sie solten in kürze ein neues Passwort per email bekommen, Sie können nach dem Anmelden in den User einstellungen ein neues Passwort wählen")
    except IndexError:
        st.warning("Falscher Username ")


def change_passwort(user_to_reset, email_to_reset, _password):

    user_to_reset_data = fetch_data(
        """SELECT * FROM mysql.users_tb WHERE username = "%s" ;""" % (user_to_reset))

    if email_to_reset == user_to_reset_data[0].get("user_email"):

        bytes = _password.encode('utf-8')
        salt = bcrypt.gensalt()

        hash = bcrypt.hashpw(bytes, salt)

        query_data("""UPDATE mysql.users_tb SET password = "%s" WHERE username = "%s" ;""" % (
            hash, user_to_reset))

        st.write(
            "Sie solten in kürze ein neues Passwort per email bekommen")
        st.write(
            "Sie können nach dem Anmelden in den User einstellungen ein neues Passwort wählen")

    else:
        st.write("Falscher Username oder Email")
