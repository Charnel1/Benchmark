from lib.db import DatabaseConnection, fetch_data, query_data
import streamlit as st
import pandas as pd

# Log Funktionen, sendet eine zeile an mysql.users_log mit User Id, der Aktion, Automatischer Timestamp und zusätzlicher Info wenn notwending


def log_User_loggin():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action) VALUES ('{st.session_state.user_id}', 'Login')")


def log_User_logout():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action) VALUES ('{st.session_state.user_id}', 'Logout')")


def log_User_FromularSpeichern():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action, INFO) VALUES ('{st.session_state.user_id}', 'Angebot speichern', 'Id: {st.session_state.Kalkulator_id}')")


def log_User_FromularLaden():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action) VALUES ('{st.session_state.user_id}', 'Angebot laden')")


def log_User_FromularSenden():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action, INFO) VALUES ('{st.session_state.user_id}', 'Angebot senden', 'Id: {st.session_state.Kalkulator_id}')")


def log_User_FromularReset():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action) VALUES ('{st.session_state.user_id}', 'Angebot reset')")


def log_User_BewertungAktualisieren():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action) VALUES ('{st.session_state.user_id}', 'Bewertung Aktualisieren')")


def log_User_AngebotLaden():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action) VALUES ('{st.session_state.user_id}', 'Angebot Laden')")


def log_User_AbrechnungenLaden():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action) VALUES ('{st.session_state.user_id}', 'Abrechnungen Laden')")


def log_User_AbrechnungenLaden_ERROR():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action) VALUES ('{st.session_state.user_id}', 'Abrechnungen Laden ERROR')")


def log_User_ProduktbewertungenAnderung(_FrageID, _Produkt, _bewertung, _beschreibung):
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action, INFO ) VALUES ('{st.session_state.user_id}', 'Produktbewertungen Änderung', '{_FrageID} - {_Produkt} - {_bewertung} - {_beschreibung}')")


def log_User_Produktbewertungen_initialisieren():
    query_data(
        f"INSERT INTO mysql.users_log (user_id, action ) VALUES ('{st.session_state.user_id}', 'Produktbewertungen initialisieren')")


def log_User_GetAllEntries():
    result = fetch_data(
        "SELECT * FROM mysql.users_log")
    print(pd.DataFrame(result))

    return pd.DataFrame(result)


def log_User_GetEntriesForDayRange(_Day_start, _Day_end):
    '''2010-03-04'''
    Start = _Day_start + " 00:00:00.000"
    End = _Day_end + " 00:00:00.000"

    result = fetch_data(
        f"SELECT * FROM mysql.users_log WHERE timestamp >= '{Start}' AND timestamp <= '{End}' ORDER BY timestamp ASC")
    print(pd.DataFrame(result))

    return pd.DataFrame(result)


def log_User_GetEntriesForDayTimeRange(_Day_start, _Day_end):
    '''2010-03-04 00:00:00.000'''
    Start = _Day_start
    End = _Day_end

    result = fetch_data(
        f"SELECT * FROM mysql.users_log WHERE timestamp >= '{Start}' AND timestamp <= '{End}' ORDER BY timestamp ASC")
    print(pd.DataFrame(result))

    return pd.DataFrame(result)


def log_UniqueUser():
    result = fetch_data(
        f"SELECT  Count(DISTINCT user_id) FROM mysql.users_log")
    print(pd.DataFrame(result))

    return pd.DataFrame(result)


def log_ForUser(userID):
    result = fetch_data(
        f"SELECT  * FROM mysql.users_log WHERE user_id = {userID}")
    print(pd.DataFrame(result))

    return pd.DataFrame(result)
