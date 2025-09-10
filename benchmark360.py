# benchmark.py
import datetime
from pathlib import Path
from functools import wraps

import bcrypt
import streamlit as st
import extra_streamlit_components as stx  # si inutilisé, vous pouvez le retirer
from streamlit_pdf_reader import pdf_reader  # si inutilisé, vous pouvez le retirer
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PARTNER_LOGO_DIR = BASE_DIR / "Partner_Logo"
DEFAULT_LOGO = BASE_DIR / "7W.png"

# --- Modules applicatifs (inchangés) ---
from lib.render import (
    render_Produkt_vergleich,
    render_admin,
    render_Angebot_history,
    render_user_settings,
    render_Abrechunungen,
    render_Produkt_bewertung,
    render_provision,
    render_position_page,
    render_user_admin_page,
    render_Batch_user_admin_page,
    render_Fragen_admin_page,
    render_Pages_admin_page,
    render_Produkt_admin_page,
    render_log_page,
)
from lib.user import reset_passwort
from lib.db import fetch_data, _fetch_data, DatabaseConnection, _query_data
from lib.log import log_User_loggin, log_User_logout
from lib.impressum import render_footer_links, render_legal_page


# =========================
# Assets: chemins robustes
# =========================
BASE_DIR = Path(__file__).resolve().parent
ASSETS_DIR = BASE_DIR / "assets"

def asset_path(name: str) -> str | None:
    """Retourne un chemin absolu vers un asset, ou None s'il est introuvable."""
    for cand in (ASSETS_DIR / name, BASE_DIR / name, Path.cwd() / name):
        if cand.is_file():
            return str(cand)
    return None

def show_image(name: str, **kwargs):
    """Affiche une image par nom d'asset, avec fallback propre."""
    p = asset_path(name)
    if p is not None:
        try:
            st.image(p, **kwargs)
            return
        except Exception:
            pass
    # Fallback
    st.warning("Konnte Logo nicht laden – Standardlogo wird verwendet.")
    fallback = asset_path("7W.png")
    if fallback:
        st.image(fallback, **kwargs)


# =========================
# Connexion DB (cache)
# =========================
@st.cache_resource
def get_db_connection():
    return DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )

db = get_db_connection()


# =========================
# Cache de données
# =========================
@st.cache_data(ttl=600)
def list_of_pages_cached(_page: str):
    _data = _fetch_data(f"SELECT username FROM mysql.user_access WHERE {_page}=1")
    return [u.get("username") for u in _data]

@st.cache_data(ttl=600)
def get_user_data(username: str):
    data = fetch_data(f"SELECT * FROM mysql.users_tb WHERE username = '{username}'")
    return data[0] if data else None

@st.cache_data(ttl=600)
def user_tag_liste_cached():
    liste = _fetch_data("SELECT DISTINCT firmen_tag FROM mysql.partner_details;")
    result_list = ['admin', 'ADMIN']
    for item in liste:
        result_list.append(item['firmen_tag'])
    return result_list


# =========================
# Session helpers
# =========================
def Set_ZohoCheckNegativ():
    if 'check_passed' not in st.session_state:
        st.session_state.check_passed = False

def get_user_Abrechnungs_status():
    # Garde la logique d'origine (connection locale)
    _db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    id_querry = _db.execute_query(
        f"SELECT id FROM mysql.Users_Tb WHERE username = '{st.session_state.get('current_username', '')}'"
    )
    respons = _db.execute_query(
        f"SELECT * FROM mysql.Abrechnungen WHERE user_id = {id_querry[0].get('id')}"
    )
    return respons

def user_tag_liste():
    liste = _fetch_data("SELECT DISTINCT firmen_tag FROM mysql.partner_details;")
    result_list = ['admin', 'ADMIN']
    for item in liste:
        result_list.append(item['firmen_tag'])
    return result_list


# =========================
# Auth décorateur
# =========================
def check_auth_user(role_required):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not st.session_state.get("logged_in", False):
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


# =========================
# Pages (wrappers)
# =========================
@check_auth_user(list_of_pages_cached("Angebotskonfigurator"))
def Vergleich_page():
    render_Produkt_vergleich()

@check_auth_user(list_of_pages_cached("Angebot_Verlauf"))
def History_page():
    Set_ZohoCheckNegativ()
    render_Angebot_history()

@check_auth_user(list_of_pages_cached("Abrechunungen"))
def Abgrechungnen_page():
    Set_ZohoCheckNegativ()
    render_Abrechunungen()

@check_auth_user(list_of_pages_cached("Admin_page"))
def Admin_page():
    Set_ZohoCheckNegativ()
    render_admin()

@check_auth_user(list_of_pages_cached("Produkt_Bewertung"))
def Produkt_bewertung_page():
    Set_ZohoCheckNegativ()
    render_Produkt_bewertung()

@check_auth_user(list_of_pages_cached("Benutzer"))
def Settings_page():
    Set_ZohoCheckNegativ()
    render_user_settings()

@check_auth_user(list_of_pages_cached("provision"))
def provision_page():
    render_provision()

@check_auth_user(list_of_pages_cached("position_page"))
def position_page():
    render_position_page()

@check_auth_user(list_of_pages_cached("user_admin"))
def user_admin_page():
    render_user_admin_page()

@check_auth_user(list_of_pages_cached("Batch_user_admin"))
def Batch_user_admin_page():
    render_Batch_user_admin_page()

@check_auth_user(list_of_pages_cached("Fragen_admin"))
def Fragen_admin_page():
    render_Fragen_admin_page()

@check_auth_user(list_of_pages_cached("Pages_admin"))
def Pages_admin_page():
    render_Pages_admin_page()

@check_auth_user(list_of_pages_cached("Produkt_admin"))
def Produkt_admin_page():
    render_Produkt_admin_page()

@check_auth_user(list_of_pages_cached("log_page"))
def log_page():
    render_log_page()

@check_auth_user(list_of_pages_cached("Abmelden"))
def Abmelden_page():
    st.session_state.logged_in = False
    log_User_logout()
    st.rerun()
    return "_"


# =========================
# Login / Reset page
# =========================
@st.fragment
def login_page():
    if st.session_state.get("logged_in") != True:
        left, right = st.columns([0.9, 0.1])
        with right:
            show_image("7W.png", width=100)

        tab1, tab2 = st.tabs(["Benutzerzugang", "Passwort zurücksetzen"])

        with tab1:
            st.title("Anmeldung")
            username_input = st.text_input("Nutzername", key="username")
            password_input = st.text_input("Passwort", type="password", key="password")

            if st.button("Anmelden"):
                try:
                    user_loggin_in = _fetch_data(
                        f"SELECT password, Aktiv FROM mysql.users_tb WHERE username = '{username_input}';"
                    )

                    if len(user_loggin_in) == 0:
                        st.error("Falscher Benutzername oder Passwort")
                        return

                    if user_loggin_in[0].get("Aktiv") != 1:
                        st.error("Benutzer deaktiviert")
                        return

                    try:
                        db_password = eval(user_loggin_in[0].get("password"))
                    except Exception:
                        st.error("Passwortfehler")
                        return

                    userBytes = password_input.encode('utf-8')

                    if bcrypt.checkpw(userBytes, db_password):
                        user_data = fetch_data(
                            f"SELECT * FROM mysql.users_tb WHERE username = '{username_input}'"
                        )[0]

                        # Set session variables
                        st.session_state.logged_in = True
                        st.session_state.user_id = user_data['id']
                        st.session_state.current_username = user_data['username']
                        st.session_state.current_Vorname = user_data['Vorname']
                        st.session_state.current_Nachname = user_data['Nachname']
                        st.session_state.current_Telefonnummer_mobil = user_data['Telefonnummer_mobil']
                        st.session_state.current_Telefonnummer_Festnetzt = user_data['Telefonnummer_festnetzt']
                        st.session_state.current_Kontak_email = user_data['kontakt_email']
                        st.session_state.current_acount_email = user_data['user_email']
                        st.session_state.current_role = user_data['role']
                        st.session_state.Zoho_Nutzer_id_Person = user_data['Zoho_Nutzer_id']

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

    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
    render_footer_links()


# =========================
# Logos par rôle
# =========================


def set_partner_logo(role: str):
    # Affichage exactement comme ta version : colonne vide + logo à droite
    empty, right = st.columns([1, 0.3])

    # Mapping comme avant : ADMIN/admin -> veovia.jpg, sinon <role>.jpg
    file_name = "veovia.jpg" if str(role).lower() == "admin" else f"{role}.jpg"

    # Chemin robuste basé sur le fichier courant (pas le CWD)
    path = PARTNER_LOGO_DIR / file_name

    # Si introuvable, on bascule silencieusement sur le logo par défaut (7W.png)
    logo_to_show = path if path.is_file() else DEFAULT_LOGO

    right.image(str(logo_to_show), width=150)



# =========================
# Application principale
# =========================
def benchmark360():
    # Init session flag
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False

    # Routes légales via query ?view=imprint|data protection
    view = st.query_params.get("view")
    if view in ("imprint", "data protection"):
        render_legal_page(view)
        return

    # Affichage logo en haut selon rôle
    current_role = st.session_state.get("current_role", "")
    current_username = st.session_state.get("current_username", "")
    
   # set_partner_logo(current_role)

    if st.session_state.logged_in:
        set_partner_logo(st.session_state.current_role)

    # Contrôle d'accès par page (basé sur la table mysql.user_access)
    PAGE_ACCESS = {
        'Angebotskonfigurator': list_of_pages_cached("Angebotskonfigurator"),
        'Angebotsverlauf':      list_of_pages_cached("Angebot_Verlauf"),
        'Abrechnungen':         list_of_pages_cached("Abrechunungen"),
        'Produktbewertungen':   list_of_pages_cached("Produkt_Bewertung"),
        'Provision':            list_of_pages_cached("provision"),
        'Access':               list_of_pages_cached("position_page"),
        'Admin page':           list_of_pages_cached("Admin_page"),
        'Benutzer':             list_of_pages_cached("Benutzer"),
        'Abmelden':             list_of_pages_cached("Abmelden"),
        'Nutzer administration':        list_of_pages_cached("user_admin"),
        'Batch Nutzer administration':  list_of_pages_cached("Batch_user_admin"),
        'Fragen administration':        list_of_pages_cached("Fragen_admin"),
        'Seiten administration':        list_of_pages_cached("Pages_admin"),
        'Produkt administration':       list_of_pages_cached("Produkt_admin"),
        'Log & Nutzer Daten':          list_of_pages_cached("log_page"),
    }

    # Objets Page
    PageObjs = {
        "Angebotskonfigurator": st.Page(Vergleich_page,                title="Angebotskonfigurator"),
        "Angebotsverlauf":      st.Page(History_page,                  title="Angebotsverlauf"),
        "Abrechnungen":         st.Page(Abgrechungnen_page,            title="Abrechnungen"),
        "Produktbewertungen":   st.Page(Produkt_bewertung_page,        title="Produktbewertungen"),
        "Provision":            st.Page(provision_page,                title="Provision"),
        "Access":               st.Page(position_page,                 title="Access"),
        "Admin page":           st.Page(Admin_page,                    title="Admin page"),
        "Benutzer":             st.Page(Settings_page,                 title="Benutzer"),
        "Nutzer administration":        st.Page(user_admin_page,       title="Nutzer administration"),
        "Batch Nutzer administration":  st.Page(Batch_user_admin_page, title="Batch Nutzer administration"),
        "Fragen administration":        st.Page(Fragen_admin_page,     title="Fragen administration"),
        "Seiten administration":        st.Page(Pages_admin_page,      title="Seiten administration"),
        "Produkt administration":       st.Page(Produkt_admin_page,    title="Produkt administration"),
        "Log & Nutzer Daten":          st.Page(log_page,              title="Log & Nutzer Daten"),
        "Abmelden":             st.Page(Abmelden_page,                 title="Abmelden"),
    }

    # Dossiers (groupes)
    folder_mapping = {
        "Angebotskonfigurator":          "Hauptfunktionen",
        "Angebotsverlauf":               "Hauptfunktionen",
        "Abrechnungen":                  "Partnerfunktionen",
        "Produktbewertungen":            "Partnerfunktionen",
        "Provision":                     "Partnerfunktionen",
        "Admin page":                    "Adminfunktionen",
        "Access":                        "Adminfunktionen",
        "Batch Nutzer administration":   "Adminfunktionen",
        "Fragen administration":         "Adminfunktionen",
        "Seiten administration":         "Adminfunktionen",
        "Nutzer administration":         "Adminfunktionen",
        "Produkt administration":        "Adminfunktionen",
        "Log & Nutzer Daten":            "Adminfunktionen",
        "Benutzer":                      "Benutzer",
        "Abmelden":                      "Benutzer",
    }

    # Page de login (si pas connecté)
    Login = st.Page(login_page, title="Anmelden")

    if st.session_state.logged_in:
        # Filtrer les pages autorisées (par username ou tag enregistré en DB)
        allowed_names = [
            page_name
            for page_name, allowed_list in PAGE_ACCESS.items()
            if (current_username in allowed_list or st.session_state.get("current_role", "") in allowed_list)
        ]

        # Construire la navigation groupée
        page_dict: dict[str, list] = {}
        for name in allowed_names:
            folder = folder_mapping.get(name)
            page_obj = PageObjs.get(name)
            if folder and page_obj:
                page_dict.setdefault(folder, []).append(page_obj)

        # Si aucune page autorisée, montrer seulement "Benutzer" et "Abmelden" si présents,
        # sinon fallback sur "Anmelden".
        if not page_dict:
            pg = st.navigation([Login])
        else:
            pg = st.navigation(page_dict, position="top", expanded=False)
    else:
        pg = st.navigation([Login])

    # Lancer la page active
    pg.run()


# =========================
# Entrée principale
# =========================
if __name__ == "__main__":
    icon = asset_path("7W.png") or "🧩"
    st.set_page_config(
        page_title="Benchmark 360",
        page_icon=icon,
        menu_items=None,
        initial_sidebar_state="expanded",
        layout="centered",
    )

    # Cacher menu/footer
    hide_streamlit_style = """
<html lang="de">
<head>
    <meta name="google" content="notranslate">
    <meta http-equiv="Content-Language" content="de-DE">
</head>
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
"""
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)

    benchmark360()
