import datetime
import bcrypt
import streamlit as st
import extra_streamlit_components as stx
import os
import time


from streamlit_pdf_reader import pdf_reader
from functools import wraps

from lib.render import render_Produkt_vergleich, render_admin, render_Angebot_history, render_user_settings, render_Abrechunungen, render_Produkt_bewertung, render_provision, render_position_page, render_user_admin_page, render_Batch_user_admin_page, render_Fragen_admin_page, render_Pages_admin_page, render_Produkt_admin_page, render_log_page
from lib.user import reset_passwort
from lib.db import fetch_data, _fetch_data, DatabaseConnection, _query_data
from lib.log import log_User_loggin, log_User_logout


def Set_ZohoCheckNegativ():
    if 'check_passed' not in st.session_state:
        st.session_state.check_passed = False
    st.session_state.check_passed = False


def get_user_Abrechnungs_status():
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
        f"SELECT id FROM mysql.Users_Tb WHERE username = '{st.session_state.current_username}'")

    respons = db.execute_query(
        f"""SELECT * FROM mysql.Abrechnungen WHERE user_id = {id_querry[0].get("id")}""")

    return respons


def user_tag_liste():
    liste = _fetch_data(
        "SELECT DISTINCT firmen_tag FROM mysql.partner_details;")
    result_list = ['admin', 'ADMIN']
    for item in liste:
        result_list.append([item][0]['firmen_tag'])

    return result_list


def check_auth(role_required):
    "x""Decorator to check user authorization for pages"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not st.session_state.logged_in:
                st.warning("Please login to access this page")
                login_page()
                return
            current_role = st.session_state.current_role
            if current_role not in role_required:
                st.error("You don't have permission to access this page")
                return

            return func(*args, **kwargs)
        return wrapper
    return decorator


def list_of_pages(_page):
    return_list = []
    _data = _fetch_data(
        f"SELECT username FROM mysql.user_access WHERE {_page}=1")
    for user in range(len(_data)):
        return_list.append(_data[user].get("username"))
    return return_list


def check_auth_user(role_required):
    """Decorator to check user authorization for pages"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not st.session_state.logged_in:
                st.warning("Please login to access this page")
                login_page()
                return
            current_username = st.session_state.current_username
            if current_username not in role_required:
                st.error("You don't have permission to access this page")
                return

            return func(*args, **kwargs)
        return wrapper
    return decorator

# --- Pages --- #


def set_partner_logo(role):
    empty, right = st.columns([1, 0.3])
    if role == "ADMIN" or role == "admin":
        _file_path = "veovia" + ".jpg"
    else:
        _file_path = format(role, "") + ".jpg"
    _path = os.path.join("Partner_Logo", _file_path)
    right.image(_path, width=150)


@st.fragment
def login_page():
    if st.session_state.logged_in != True:
        left, right = st.columns([0.9, 0.1])
        right.image(
            "7W.png", width=100)
        tab1, tab2 = st.tabs(["Benutzerzugang", "Passwort zurücksetzen"])
        with tab1:
            st.title("Anmeldung")
            username_input = st.text_input("Nutzername", key="username")
            password_input = st.text_input(
                label="Passwort", type="password", key="password")

            if st.button("Anmelden"):
                try:
                    user_loggin_in = _fetch_data(
                        f"SELECT password, Aktiv FROM mysql.users_tb WHERE username = '{username_input}';")
                    if user_loggin_in[0].get("Aktiv") == 1:
                        try:
                            db_password = eval(
                                user_loggin_in[0].get("password"))
                        except IndexError:
                            st.error(
                                'Falscher Benutzername oder Passwort')

                        if len(list((user_loggin_in))) > 0:
                            userBytes = password_input.encode('utf-8')
                            # In production, use bcrypt.checkpw()
                            if bcrypt.checkpw(userBytes, db_password) == True:
                                user_data = fetch_data(
                                    f"SELECT * FROM mysql.users_tb WHERE username = '{st.session_state.username}'")
                                st.session_state.logged_in = True
                                st.session_state.user_id = user_data[0]['id']
                                st.session_state.current_username = user_data[0]['username']
                                st.session_state.current_Vorname = user_data[0]['Vorname']
                                st.session_state.current_Nachname = user_data[0]['Nachname']
                                st.session_state.current_Telefonnummer_mobil = user_data[
                                    0]['Telefonnummer_mobil']
                                st.session_state.current_Telefonnummer_Festnetzt = user_data[
                                    0]['Telefonnummer_festnetzt']
                                st.session_state.current_Kontak_email = user_data[0]['kontakt_email']
                                st.session_state.current_acount_email = user_data[0]['user_email']
                                st.session_state.current_role = user_data[0]['role']
                                st.session_state.Zoho_Nutzer_id_Person = user_data[0]['Zoho_Nutzer_id']

                                # Geschäftsebene

                                st.success("Erfolgreich angemeldet!")
                                log_User_loggin()

                                st.rerun()

                            else:
                                st.error("Falscher Benutzername oder Passwort")
                except IndexError:
                    st.error("Falscher Benutzername oder Passwort")

        with tab2:
            st.title("Neues Passwort Anfordern")
            user_to_reset = st.text_input("Username", key="username_reset")

            if st.button("Reset"):
                reset_passwort(user_to_reset)


@check_auth_user(list_of_pages("Angebotskonfigurator"))
def Vergleich_page():
    render_Produkt_vergleich()


@check_auth_user(list_of_pages("Angebot_Verlauf"))
def History_page():
    Set_ZohoCheckNegativ()
    render_Angebot_history()


@check_auth_user(list_of_pages("Abrechunungen"))
def Abgrechungnen_page():
    Set_ZohoCheckNegativ()
    render_Abrechunungen()


@check_auth_user(list_of_pages("Admin_page"))
def Admin_page():
    Set_ZohoCheckNegativ()
    render_admin()


@check_auth_user(list_of_pages("Produkt_Bewertung"))
def Produkt_bewertung_page():
    Set_ZohoCheckNegativ()
    render_Produkt_bewertung()


@check_auth_user(list_of_pages("Benutzer"))
def Settings_page():
    Set_ZohoCheckNegativ()
    render_user_settings()


@check_auth_user(list_of_pages("provision"))
def provision_page():
    render_provision()


@check_auth_user(list_of_pages("position_page"))
def position_page():
    render_position_page()


@check_auth_user(list_of_pages("user_admin"))
def user_admin_page():
    render_user_admin_page()


@check_auth_user(list_of_pages("Batch_user_admin"))
def Batch_user_admin_page():
    render_Batch_user_admin_page()


@check_auth_user(list_of_pages("Fragen_admin"))
def Fragen_admin_page():
    render_Fragen_admin_page()


@check_auth_user(list_of_pages("Pages_admin"))
def Pages_admin_page():
    render_Pages_admin_page()

#


@check_auth_user(list_of_pages("Produkt_admin"))
def Produkt_admin_page():
    render_Produkt_admin_page()


@check_auth_user(list_of_pages("log_page"))
def log_page():
    render_log_page()


@check_auth_user(list_of_pages("Abmelden"))
def Abmelden_page():
    st.session_state.logged_in = False
    st.rerun()
    return "_"

    # --- MAIN --- #


def benchmark360():

    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False

    # ROLES ##
    PAGE_ACCESS = {
        'Angebotskonfigurator': list_of_pages("Angebotskonfigurator"),
        'Angebotsverlauf': list_of_pages("Angebot_Verlauf"),
        'Abrechnungen': list_of_pages("Abrechunungen"),
        'Produktbewertungen': list_of_pages("Produkt_Bewertung"),
        'Provision': list_of_pages("provision"),
        'Access': list_of_pages("position_page"),
        'Admin page': list_of_pages("Admin_page"),
        'Benutzer': list_of_pages("Benutzer"),
        'Abmelden': list_of_pages("Abmelden"),
        'Nutzer administration': list_of_pages("user_admin"),
        'Batch Nutzer administration': list_of_pages("Batch_user_admin"),
        'Fragen administration': list_of_pages("Fragen_admin"),
        'Seiten administration': list_of_pages("Pages_admin"),
        'Produkt administration': list_of_pages("Produkt_admin"),
        'Log & Nutzer Daten': list_of_pages("log_page"),

    }
    Login = st.Page(login_page, title="Anmelden")

    if st.session_state.logged_in:
        Angebotskonfigurator = st.Page(
            Vergleich_page, title="Angebotskonfigurator")

        def Angebotsverlauf():
            History_page()

        def Abmelden():
            Abmelden_page()

        def Abrechnungen():
            Abgrechungnen_page()

        def Provision():
            provision_page()

        def Produktbewertungen():
            Produkt_bewertung_page()

        def Access():
            render_position_page()

        def ADMIN():
            render_admin()

        def Einstellungen():
            Settings_page()

        def MultiNutzerAdmin():
            render_Batch_user_admin_page()

        def FragenAdmin():
            render_Fragen_admin_page()

        def SeitenAdmin():
            render_Pages_admin_page()

        def SigleNutzerAdmin():
            render_user_admin_page()

        def ProduktAdmin():
            Produkt_admin_page()

        def LogUserData():
            log_page()

        Verlauf = st.Page(Angebotsverlauf, title="Angebotsverlauf")

        Abmelden = st.Page(Abmelden, title="Abmelden")

        Abrechnungen = st.Page(Abrechnungen, title="Abrechnungen")

        Provision = st.Page(Provision, title="Provision")

        Produktbewertungen = st.Page(
            Produktbewertungen, title="Produktbewertungen")

        position_page = st.Page(Access, title="Access")

        Admin_page = st.Page(ADMIN, title="Main Admin page")

        Einstellungen = st.Page(Einstellungen, title="Einstellungen")

        Batch_Nutzer_erstellung = st.Page(
            MultiNutzerAdmin, title="Multi Nutzer erstellung")

        Fragen_administration = st.Page(
            FragenAdmin, title="Fragen administration")

        Seiten_administration = st.Page(
            SeitenAdmin, title="Seiten administration")

        Single_Nutzer_erstellung = st.Page(
            SigleNutzerAdmin, title="Single Nutzer erstellung")

        Produkt_administration = st.Page(
            ProduktAdmin, title="Produkt administration")

        Log_Nutzer_Daten = st.Page(LogUserData, title="Log & Nutzer Daten")

        if 'sidebar_state' not in st.session_state:
            st.session_state.sidebar_state = 'expanded'

        current_role = st.session_state.current_role
        current_username = st.session_state.current_username
        pages = {page: roles for page, roles in PAGE_ACCESS.items()
                 if current_role in roles or current_username in roles}

        page_dict = {}
        # Folder count if 0 Erstellen
        # else append
        Hauptfunktionen_Counter = 0
        Partnerfunktionen_Counter = 0
        Adminfunktionen_Counter = 0
        Benutzer_Counter = 0

        # Sort Pages to Access
        for item in list(pages.keys()):
            match item:
                # MAIN FUNKTIONEN
                case "Angebotskonfigurator":
                    if Hauptfunktionen_Counter == 0:
                        page_dict["Hauptfunktionen"] = [Angebotskonfigurator]
                        Hauptfunktionen_Counter += 1
                    else:
                        page_dict["Hauptfunktionen"].append(
                            Angebotskonfigurator)

                case "Angebotsverlauf":
                    if Hauptfunktionen_Counter == 0:
                        page_dict["Hauptfunktionen"] = [Verlauf]
                        Hauptfunktionen_Counter += 1
                    else:
                        page_dict["Hauptfunktionen"].append(Verlauf)

                # PARTNER FUNKTIONEN
                case "Abrechnungen":
                    if Partnerfunktionen_Counter == 0:
                        page_dict["Partnerfunktionen"] = [Abrechnungen]
                        Partnerfunktionen_Counter += 1
                    else:
                        page_dict["Partnerfunktionen"].append(Abrechnungen)

                # ADMIN FUNKTIONEN
                case "Admin page":
                    if Adminfunktionen_Counter == 0:
                        page_dict["Adminfunktionen"] = [Admin_page]
                        Adminfunktionen_Counter += 1
                    else:
                        page_dict["Adminfunktionen"].append(Admin_page)

                case "Access":
                    if Adminfunktionen_Counter == 0:
                        page_dict["Adminfunktionen"] = [position_page]
                        Adminfunktionen_Counter += 1
                    else:
                        page_dict["Adminfunktionen"].append(position_page)

                case "Batch Nutzer administration":
                    if Adminfunktionen_Counter == 0:
                        page_dict["Adminfunktionen"] = [
                            Batch_Nutzer_erstellung]
                        Adminfunktionen_Counter += 1
                    else:
                        page_dict["Adminfunktionen"].append(
                            Batch_Nutzer_erstellung)

                case "Fragen administration":
                    if Adminfunktionen_Counter == 0:
                        page_dict["Adminfunktionen"] = [Fragen_administration]
                        Adminfunktionen_Counter += 1
                    else:
                        page_dict["Adminfunktionen"].append(
                            Fragen_administration)

                case "Seiten administration":
                    if Adminfunktionen_Counter == 0:
                        page_dict["Adminfunktionen"] = [Seiten_administration]
                        Adminfunktionen_Counter += 1
                    else:
                        page_dict["Adminfunktionen"].append(
                            Seiten_administration)

                case "Nutzer administration":
                    if Adminfunktionen_Counter == 0:
                        page_dict["Adminfunktionen"] = [
                            Single_Nutzer_erstellung]
                        Adminfunktionen_Counter += 1
                    else:
                        page_dict["Adminfunktionen"].append(
                            Single_Nutzer_erstellung)

                case "Produkt administration":
                    if Adminfunktionen_Counter == 0:
                        page_dict["Adminfunktionen"] = [Produkt_administration]
                        Adminfunktionen_Counter += 1
                    else:
                        page_dict["Adminfunktionen"].append(
                            Produkt_administration)

                case "Log & Nutzer Daten":
                    if Adminfunktionen_Counter == 0:
                        page_dict["Adminfunktionen"] = [Log_Nutzer_Daten]
                        Adminfunktionen_Counter += 1
                    else:
                        page_dict["Adminfunktionen"].append(Log_Nutzer_Daten)

                # Benutzer FUNKTIONEN
                case "Benutzer":
                    if Benutzer_Counter == 0:
                        page_dict["Benutzer"] = [Einstellungen]
                        Benutzer_Counter += 1
                    else:
                        page_dict["Benutzer"].append(Einstellungen)

                case "Abmelden":
                    if Benutzer_Counter == 0:
                        page_dict["Benutzer"] = [Abmelden]
                        Benutzer_Counter += 1
                    else:
                        page_dict["Benutzer"].append(Abmelden)

            #

        # Seiten administration
        # page_dict["Benutzer"] = [Abmelden]

        set_partner_logo(current_role)

        # selection = st.sidebar.pills(
        #    "**Navigation:**", list(pages.keys()), default=list(pages.keys())[0])

        pg = st.navigation(page_dict, position="top", expanded=False)

    else:
        pg = st.navigation([Login])
    pg.run()


if __name__ == "__main__":
    st.set_page_config(
        page_title="Benchmark 360", page_icon="7W.png", menu_items=None, initial_sidebar_state="expanded", layout="centered"
    )
    hide_streamlit_style = """

<html lang="de">
<head>
    <meta name="google" content="notranslate">
    <meta http-equiv="Content-Language" content="de-DE">
    <!-- Your other head elements -->
</head>
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>

"""
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)

    benchmark360()
