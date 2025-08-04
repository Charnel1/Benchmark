import streamlit as st
import mysql.connector
from datetime import datetime
from dateutil.relativedelta import relativedelta


from .Comparison import ComparisonState
from lib.db import DatabaseConnection


options_dict = {
    'wichtig': ComparisonState.Wichtig,
    'Optional': ComparisonState.Optional,
    'unwichtig': ComparisonState.Unwichtig
}


def NotEmpty(_key):
    if st.session_state[_key] == None:
        st.session_state[_key] = "unwichtig"


def update_depedent_nmb():
    Sonderfall = {"S22": "Q30",  # Anzahl Nutzer MS Teams, zusätzlich zu Nebenstelle
                  "S24": "Q17",  # Anzahl Dialer Verwendung in MS Teams Nebenstelle
                  "S25": "Q38",  # Anzahl Präsenzabgleich zu MS Teams
                  "S26": "Q35",  # Anzahl iOS Nutzer
                  "S27": "Q34",  # Anzahl Andorid Nutzer
                  "S24": "Q17",  # Anzahl Dialer Verwendung in MS Teams Nebenstelle
                  "S38": "Q2",   # Anzahl analoge Faxgeräte
                  }

    for keys, value in Sonderfall.items():
        for Q in range(len(Ask_Anzahl._registry)):
            if Ask_Anzahl._registry[Q].key == keys:
                if st.session_state[Ask_Anzahl._registry[Q].key] != None:
                    if st.session_state[Ask_Anzahl._registry[Q].key] >= 1:

                        for items in range(len(Ask_Question._registry)):
                            if Ask_Question._registry[items].key == value:
                                Q_key = Ask_Question._registry[items].key
                                st.session_state[Q_key] = "wichtig"
                                Ask_Question._registry[items].set_default(
                                    "wichtig")
                                Ask_Question._registry[items].set_awnser(
                                    "wichtig")
                    elif st.session_state[Ask_Anzahl._registry[Q].key] < 1 or "":
                        for items in range(len(Ask_Question._registry)):
                            if Ask_Question._registry[items].key == value:

                                Q_key = Ask_Question._registry[items].key
                                st.session_state[Q_key] = "unwichtig"
                                Ask_Question._registry[items].set_default(
                                    "unwichtig")
                                Ask_Question._registry[items].set_awnser(
                                    "unwichtig")


def update_depedent_opt():

    if st.session_state.S80 == True:
        st.session_state.Q32 = "wichtig"
    elif st.session_state.S80 == False:
        st.session_state.Q32 = "unwichtig"
    elif st.session_state.S81 == True:
        st.session_state.Q33 = "wichtig"
    elif st.session_state.S81 == False:
        st.session_state.Q33 = "unwichtig"

    # S20


def update_depedent_Q_frage_nmb():

    for Q in range(len(Ask_Question._registry)):

        if "Q31" == Ask_Question._registry[Q].key:
            if st.session_state.Q31 == "wichtig":
                # Nachträgliche Analyse - Anzahl Nutzer
                for S in range(len(Ask_Anzahl._registry)):
                    if Ask_Anzahl._registry[S].key == "S57":
                        Ask_Anzahl._registry[S].ask_nmb()
                    if Ask_Anzahl._registry[S].key == "S11":
                        Ask_Anzahl._registry[S].ask_nmb()
                st.rerun()

        if "Q14" == Ask_Question._registry[Q].key:
            if st.session_state.Q14 == "wichtig":
                # Contact Center
                for S in range(len(Ask_Anzahl._registry)):
                    if Ask_Anzahl._registry[S].key == "S58":
                        Ask_Anzahl._registry[S].ask_nmb()


class Ask_Question:
    '''
    Q - Fragen; werden für denn Bewertungsvergleich benutzt
    '''
    _registry = []

    def __init__(self, key, name, help, options, default):

        self.key = key
        self.name = name
        self.help = help
        self.options = options
        self.default = default
        self.awnser = "unwichtig"

        if not any(_registry.key == self.key for _registry in Ask_Question._registry):
            Ask_Question._registry.append(self)

    @st.fragment()
    def ask(self):

        if self.key not in st.session_state:
            st.session_state[f'{self.key}'] = "unwichtig"

        if self.key == "Q32":
            if st.session_state.S80 == True:
                st.session_state[f'{self.key}'] = "wichtig"
            else:
                st.session_state[f'{self.key}'] = "unwichtig"
        if self.key == "Q33":
            if st.session_state.S81 == True:
                st.session_state[f'{self.key}'] = "wichtig"
            else:
                st.session_state[f'{self.key}'] = "unwichtig"

        if self.key == "Q35":  # IOS
            if st.session_state.S26 > 0 and st.session_state.S26 != False:
                st.session_state[f'{self.key}'] = "wichtig"
        if self.key == "Q34":  # Android
            if st.session_state.S27 > 0 and st.session_state.S27 != False:
                st.session_state[f'{self.key}'] = "wichtig"

        _L = "O_" + self.key
        _L_bool = "loader_" + self.key

        if _L_bool in st.session_state:
            if _L_bool in st.session_state:
                if st.session_state[_L_bool]:
                    if st.session_state[_L] != "unwichtig":
                        if st.session_state[_L] != "False":
                            if st.session_state[_L] != False:
                                st.session_state[self.key] = st.session_state[_L]
                                st.session_state[_L_bool] = False

        self.awnser = st.segmented_control(
            key=self.key, label=self.name, options=list(options_dict.keys()), help=self.help, on_change=NotEmpty(self.key))

    @st.fragment()
    def ask_invis(self):
        if self.key not in st.session_state:
            st.session_state[self.key] = "unwichtig"
        if self.key == "Q32":
            if st.session_state.S80 == True:
                st.session_state[f'{self.key}'] = "wichtig"
            else:
                st.session_state[f'{self.key}'] = "unwichtig"
        if self.key == "Q33":
            if st.session_state.S81 == True:
                st.session_state[f'{self.key}'] = "wichtig"
            else:
                st.session_state[f'{self.key}'] = "unwichtig"

        self.awnser = st.session_state[self.key]

    def set_awnser(self, new_awnser):
        self.awnser = new_awnser

    def set_default(self, new_default):
        self.default = new_default


class Ask_Anzahl:  # S21
    '''
    S - Fragen; Werden nur für anzahl benutzt
    K - Fragen; Sind Kunden angaben
    '''
    _registry = []

    def __init__(self, key, name, help):

        self.help = help
        self.key = key
        self.name = name
        self.awnser = ""
        if not any(_registry.key == self.key for _registry in Ask_Anzahl._registry):
            Ask_Anzahl._registry.append(self)

    @st.fragment()
    def ask_nmb(self):
        if self.key not in st.session_state:
            st.session_state[f'{self.key}'] = 0

        _L = "O_" + self.key
        _L_bool = "loader_" + self.key
        if _L_bool in st.session_state:
            if _L_bool in st.session_state:
                if st.session_state[_L_bool]:
                    if st.session_state[_L] != "False":
                        st.session_state[self.key] = int(st.session_state[_L])
                        st.session_state[_L_bool] = False

        awnser = st.number_input(
            key=self.key, label=self.name, min_value=0, max_value=99999, help=self.help, on_change=update_depedent_opt)
        # st.session_state[str_format] = st.session_state[self.key]

        self.awnser = awnser

    @st.fragment()
    def ask_checkbox(self):
        if self.key not in st.session_state:
            st.session_state[f'{self.key}'] = False

        _L = "O_" + self.key
        _L_bool = "loader_" + self.key
        if _L_bool in st.session_state:
            if _L_bool in st.session_state:
                if st.session_state[_L_bool]:
                    st.session_state[self.key] = st.session_state[_L]
                    st.session_state[_L_bool] = False

        self.awnser = st.checkbox(key=self.key, label=self.name,
                                  on_change=update_depedent_opt)

    @st.fragment()
    def ask_select(self, opt_list):
        if self.key not in st.session_state:
            st.session_state[f'{self.key}'] = opt_list[0]

        _L = "O_" + self.key
        _L_bool = "loader_" + self.key
        if _L_bool in st.session_state:
            if _L_bool in st.session_state:
                if st.session_state[_L_bool]:
                    if st.session_state[_L] != "False":
                        if st.session_state[_L] != False:
                            st.session_state[self.key] = st.session_state[_L]
                            st.session_state[_L_bool] = False

        self.awnser = st.selectbox(key=self.key, label=self.name, options=opt_list,
                                   on_change=update_depedent_opt)
        return self.awnser

    @st.fragment()
    def ask_nmb_invis(self):
        awnser = st.number_input(
            key=self.key, label=self.name, min_value=0, max_value=200, value=None, step=1, help=self.help, on_change=update_depedent_nmb, disabled=True)
        self.awnser = awnser

    @st.fragment()
    def ask_date(self, early_date):
        if self.key not in st.session_state:
            st.session_state[f'{self.key}'] = early_date

        _L = "O_" + self.key
        _L_bool = "loader_" + self.key
        if _L_bool in st.session_state:
            if _L_bool in st.session_state:
                if st.session_state[_L_bool]:
                    _format = "%Y-%m-%d"
                    Obj_DateTime = datetime.strptime(
                        st.session_state[_L], _format)

                    st.session_state[self.key] = Obj_DateTime.date()
                    st.session_state[_L_bool] = False

        awnser = st.date_input(
            key=self.key, label=self.name, help=self.help, format="YYYY-MM-DD")
        self.awnser = awnser

        return awnser

    @st.fragment()
    def speziell_ask_drop(self):
        str_format = "O_" + self.key
        if str_format in st.session_state:
            awnser = st.selectbox(
                key=self.key, label=self.name, help=self.help, options=["nicht gewünscht", "36 Monate", "48 Monate", "60 Monate"], index=st.session_state[str_format])
            awnser = st.session_state[str_format]
        else:
            awnser = st.selectbox(
                key=self.key, label=self.name, help=self.help, options=["nicht gewünscht", "36 Monate", "48 Monate", "60 Monate"])
        self.awnser = awnser

    @st.fragment()
    def ask(self):
        if self.key not in st.session_state:
            st.session_state[f'{self.key}'] = self.default
        _L = "O_" + self.key
        _L_bool = "loader_" + self.key
        if _L_bool in st.session_state:
            if _L_bool in st.session_state:
                if st.session_state[_L_bool]:
                    if st.session_state[_L] != "False":
                        if st.session_state[_L] != False:
                            st.session_state[self.key] = format(
                                st.session_state[_L], "")
                            st.session_state[_L_bool] = False

        awnser = st.segmented_control(
            key=self.key,
            label=self.name, options=self.options, help=self.help, selection_mode=self.mode, on_change=update_depedent_opt)
        self.awnser = awnser

    def init_ask(self, options, default, mode):
        self.options = options
        self.default = default
        self.mode = mode

    @st.fragment()
    def ask_text(self):
        if self.key not in st.session_state:
            st.session_state[f'{self.key}'] = " "
        _L = "O_" + self.key
        _L_bool = "loader_" + self.key
        if _L_bool in st.session_state:
            if _L_bool in st.session_state:
                if st.session_state[_L_bool]:
                    if st.session_state[_L] != "False":
                        if st.session_state[_L] != False:
                            st.session_state[self.key] = format(
                                st.session_state[_L], "")
                            st.session_state[_L_bool] = False

        awnser = st.text_input(
            label=self.name, key=self.key)
        self.awnser = awnser
        return awnser


@st.cache_data
def get_Q_data():
    db = mysql.connector.connect(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        port="3306",
        password="vv_webapp_2025"
    )
    db.connect()
    cursor = db.cursor()
    # resources

    cursor.execute("SELECT *, Options FROM mysql.Fragen")
    meta_question_list = cursor.fetchall()
    cursor.close()
    db.close()
    return meta_question_list


@st.cache_data
def get_every_Group():
    db = mysql.connector.connect(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        port="3306",
        password="vv_webapp_2025"
    )
    db.connect()
    cursor = db.cursor()
    # resources

    cursor.execute(
        "SELECT DISTINCT Gruppe FROM mysql.Fragen WHERE Gruppe IS NOT NULL")  #
    gruppen_liste = cursor.fetchall()
    cursor.close()
    db.close()
    return gruppen_liste


@st.cache_data
def init_Questions(fragen_liste: list):
    fragen_liste_key = []
    for items in range(len(fragen_liste)):

        if "Q" in fragen_liste[items][4]:
            Ask_Question(fragen_liste[items][1],
                         fragen_liste[items][2],
                         fragen_liste[items][3], fragen_liste[items][5].split(
                             ", "),
                         fragen_liste[items][6])
            fragen_liste_key.append(fragen_liste[items][1])
        if "ask_nmb()" == fragen_liste[items][4]:
            Ask_Anzahl(fragen_liste[items][1],
                       fragen_liste[items][2], fragen_liste[items][3])
            fragen_liste_key.append(fragen_liste[items][1])

        if "ask_q()" == fragen_liste[items][4]:
            Ask_Anzahl(fragen_liste[items][1],
                       fragen_liste[items][2], fragen_liste[items][3]).init_ask(fragen_liste[items][5].split(", "), fragen_liste[items][6], "single")

        if "ask_text()" == fragen_liste[items][4]:
            Ask_Anzahl(fragen_liste[items][1],
                       fragen_liste[items][2], fragen_liste[items][3])

            fragen_liste_key.append(fragen_liste[items][1])
    return fragen_liste_key


@st.cache_data
def get_group_by(colum_name: str, OrderBy: str, where: str):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        port="3306",
        password="vv_webapp_2025"
    )
    db.connect()
    data = db.execute_query("SELECT %s FROM mysql.Fragen WHERE %s Order By %s ASC" % (
        colum_name, where, OrderBy))
    db.disconnect()

    return data


@st.cache_data
def get_group_position():
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        port="3306",
        password="vv_webapp_2025"
    )
    db.connect()
    data = db.execute_query(
        "SELECT Gruppe, MIN(Gruppen_P) AS min_position FROM mysql.Fragen GROUP BY Gruppe Order By min_position ASC")
    db.disconnect()

    return data


@st.fragment
def ask_every_Q_with_key_name_return():
    '''
    Erstellt das Fromular der Fragen und returend die Antworten in einem Dict mit Key - ID 
    '''
    gruppen = get_group_position()
    meta_question_list = get_Q_data()
    key_liste = init_Questions(meta_question_list)
    # gruppe = "Grundfunktionen"
    # st.write(get_group_by(
    #    "Key_name", "Gruppen_P, Fragen_P", f"Gruppe = '{gruppe}'"))

    Key_Group = {}
    Key_Type = {}

    for item in range(len(meta_question_list)):
        Key_Group[meta_question_list[item][1]] = meta_question_list[item][7]
        Key_Type[meta_question_list[item][1]] = meta_question_list[item][4]

    for items in gruppen:

        if items.get("Gruppe") is not None:
            titel = items.get("Gruppe")

            with st.expander(titel):
                st.markdown("")
                st.markdown("")
                fragen = get_group_by("Key_name", "Fragen_P",
                                      f"Gruppe = '{titel}'")

                for Q_key in fragen:
                    Fragen_key = Q_key.get('Key_name')
                    for Q in range(len(Ask_Question._registry)):
                        if Fragen_key == Ask_Question._registry[Q].key:
                            if Fragen_key == "Q2" or Fragen_key == "Q34" or Fragen_key == "Q35" or Fragen_key == "Q44" or Fragen_key == "Q50" or Fragen_key == "Q64" or Fragen_key == "Q32" or Fragen_key == "Q33":
                                Ask_Question._registry[Q].ask_invis()
                            else:
                                col1, col2, col3 = st.columns(
                                    [0.4, 0.3, 0.3], vertical_alignment="center", gap="small")
                                with col1:
                                    Ask_Question._registry[Q].ask()

                                ### SPEZIAL FÄLLE ###

                                if "Q27" == Ask_Question._registry[Q].key:
                                    for S in range(len(Ask_Anzahl._registry)):
                                        with col2:
                                            if Ask_Anzahl._registry[S].key == "S12":
                                                Ask_Anzahl._registry[S].ask_nmb(
                                                )
                                        with col3:
                                            if Ask_Anzahl._registry[S].key == "S13":
                                                Ask_Anzahl._registry[S].ask_nmb(
                                                )

                                if "Q31" == Ask_Question._registry[Q].key:
                                    for S in range(len(Ask_Anzahl._registry)):
                                        with col2:
                                            if Ask_Anzahl._registry[S].key == "S57":
                                                Ask_Anzahl._registry[S].ask_nmb(
                                                )
                                        with col3:
                                            if Ask_Anzahl._registry[S].key == "S11":
                                                Ask_Anzahl._registry[S].ask_nmb(
                                                )
                                if "Q14" == Ask_Question._registry[Q].key:
                                    for S in range(len(Ask_Anzahl._registry)):
                                        with col3:
                                            if Ask_Anzahl._registry[S].key == "S58":
                                                Ask_Anzahl._registry[S].ask_nmb(
                                                )
                                if "Q47" == Ask_Question._registry[Q].key:
                                    for S in range(len(Ask_Anzahl._registry)):
                                        with col3:
                                            if Ask_Anzahl._registry[S].key == "S16":
                                                Ask_Anzahl._registry[S].ask_nmb(
                                                )
                                if "Q24" == Ask_Question._registry[Q].key:
                                    for S in range(len(Ask_Anzahl._registry)):
                                        with col3:
                                            if Ask_Anzahl._registry[S].key == "S14":
                                                Ask_Anzahl._registry[S].ask_nmb(
                                                )

                                if "Q49" == Ask_Question._registry[Q].key:
                                    for S in range(len(Ask_Anzahl._registry)):
                                        with col3:
                                            if Ask_Anzahl._registry[S].key == "S10":
                                                Ask_Anzahl._registry[S].ask_nmb(
                                                )
                                if "S40" == Ask_Question._registry[Q].key:
                                    for S in range(len(Ask_Anzahl._registry)):
                                        with col3:
                                            if Ask_Anzahl._registry[S].key == "S40":
                                                Ask_Anzahl._registry[S].ask_nmb_invis(
                                                )
                                if "S40" == Ask_Question._registry[Q].key:
                                    for S in range(len(Ask_Anzahl._registry)):
                                        with col3:
                                            if Ask_Anzahl._registry[S].key == "S40":
                                                Ask_Anzahl._registry[S].ask_nmb_invis(
                                                )

                    # S
                    for Q in range(len(Ask_Anzahl._registry)):
                        if Fragen_key == Ask_Anzahl._registry[Q].key:
                            if Fragen_key == "S55":
                                Ask_Anzahl._registry[Q].speziell_ask_drop()
                            else:
                                if Fragen_key == "S20":
                                    st.markdown("Nutzung auf Computer per:")

                                    @st.fragment
                                    def ask_Computer():
                                        left, mid, right = st.columns(
                                            [0.3, 0.5, 0.5])
                                        with mid:

                                            Ask_Anzahl(
                                                key="S80", name="APP", help="").ask_checkbox()
                                        with right:
                                            Ask_Anzahl(
                                                key="S81", name="WebRTC", help="").ask_checkbox()

                                    ask_Computer()

                                else:
                                    if Fragen_key != "S12" and Fragen_key != "S13" and Fragen_key != "S57" and Fragen_key != "S11" and Fragen_key != "S58" and Fragen_key != "S16" and Fragen_key != "S14" and Fragen_key != "S10":
                                        if Key_Type[Ask_Anzahl._registry[Q].key] == "ask_nmb()" or Key_Type[Ask_Anzahl._registry[Q].key] == "ask_text()":
                                            left, right = st.columns(
                                                [0.5, 0.1], vertical_alignment="bottom")

                                            @st.fragment
                                            def Opt_check(_key):

                                                return Ask_Anzahl(key=_key, name="Optional", help="")

                                            if Fragen_key == "S28":
                                                with left:
                                                    Ask_Anzahl._registry[Q].ask_nmb(
                                                    )
                                                with right:
                                                    Opt_check(
                                                        "S90").ask_checkbox()
                                            if Fragen_key == "S29":
                                                with left:
                                                    Ask_Anzahl._registry[Q].ask_nmb(
                                                    )
                                                with right:
                                                    Opt_check(
                                                        "S91").ask_checkbox()
                                            if Fragen_key == "S30":
                                                with left:
                                                    Ask_Anzahl._registry[Q].ask_nmb(
                                                    )
                                                with right:
                                                    Opt_check(
                                                        "S92").ask_checkbox()
                                            if Fragen_key == "S31":
                                                with left:
                                                    Ask_Anzahl._registry[Q].ask_nmb(
                                                    )
                                                with right:
                                                    Opt_check(
                                                        "S93").ask_checkbox()
                                            if Fragen_key == "S32":
                                                with left:
                                                    Ask_Anzahl._registry[Q].ask_nmb(
                                                    )
                                                with right:
                                                    Opt_check(
                                                        "S94").ask_checkbox()
                                            if Fragen_key == "S33":
                                                with left:
                                                    Ask_Anzahl._registry[Q].ask_nmb(
                                                    )
                                                with right:
                                                    Opt_check(
                                                        "S95").ask_checkbox()
                                            if Fragen_key == "S43" or Fragen_key == "S42":
                                                if Fragen_key == "S42":
                                                    CTI_Liste = [
                                                        "",
                                                        r"11880.com (ehem. klickTel Server)",
                                                        r"1CRM",
                                                        r"4me",
                                                        r"911inform Location Discovery Solution",
                                                        r"Act12010+",
                                                        r"ActiveFax Server",
                                                        r"adress PLUS",
                                                        r"AI-Powered Call Analytics Velvetech",
                                                        r"Aigent Real-time Assistant",
                                                        r"Airbrake",
                                                        r"Akazio",
                                                        r"All Bot",
                                                        r"Anti-scam Shield",
                                                        r"Apple Adressbuch (macOS)",
                                                        r"AppSignal",
                                                        r"Apptivo",
                                                        r"arc Voice & Video Analytics",
                                                        r"Archiver",
                                                        r"Asana Bot",
                                                        r"Auditwise",
                                                        r"AutoReach",
                                                        r"Autotask",
                                                        r"Baritrax360",
                                                        r"Beetexting",
                                                        r"Bitbucket",
                                                        r"Bitrix24 CRM",
                                                        r"Boomtown",
                                                        r"Box",
                                                        r"bpm'online",
                                                        r"Bridge Call Queue Alert - Add-In",
                                                        r"Bridge Operator Console",
                                                        r"Bubble",
                                                        r"Bugsnag",
                                                        r"Built.io Flow",
                                                        r"Bullhorn",
                                                        r"CalendarHero",
                                                        r"Call AI",
                                                        r"Call Connect GP (Patient Connect)",
                                                        r"Call Optimizer",
                                                        r"CallSights",
                                                        r"Candor",
                                                        r"Canvas",
                                                        r"Carerix",
                                                        r"CAS Genesis World",
                                                        r"CEIPAL ATS",
                                                        r"Central Desk",
                                                        r"charly",
                                                        r"ChatrHub",
                                                        r"Chexout Partner App",
                                                        r"Chili Piper",
                                                        r"Chorus.ai",
                                                        r"ChronicCareIQ",
                                                        r"Circle CI",
                                                        r"CirQlive",
                                                        r"Citrix Phone Plugin",
                                                        r"Citrix RingCentral App",
                                                        r"Cleanshelf App",
                                                        r"Clerk - Slack SMS",
                                                        r"Clio Call Connector",
                                                        r"CliqData",
                                                        r"Cloze",
                                                        r"Cloze Bot",
                                                        r"Cobra CRM PRO/PLUS",
                                                        r"Codeship",
                                                        r"Cognitive View",
                                                        r"Conduit Office",
                                                        r"Confluence",
                                                        r"Connecsy Attendant",
                                                        r"Connectivity",
                                                        r"ConnectWise",
                                                        r"ContactPro",
                                                        r"ConvergeHub",
                                                        r"Copper",
                                                        r"Crashlytics",
                                                        r"Creatio",
                                                        r"Crelate",
                                                        r"Cron Bot",
                                                        r"Das Telefonbuch des TVG Verlags",
                                                        r"Datadog",
                                                        r"DATEV",
                                                        r"Dezrez",
                                                        r"DialogTech",
                                                        r"Digital Concierge",
                                                        r"Digno",
                                                        r"Donedone",
                                                        r"Dropbox",
                                                        r"Dubber",
                                                        r"eAgent Screen Pop",
                                                        r"EcoLink - AMS360",
                                                        r"Egroupware",
                                                        r"enable.us",
                                                        r"Enthu.ai",
                                                        r"Envoy",
                                                        r"Envoy TAXI",
                                                        r"ES Office",
                                                        r"Evernote",
                                                        r"Exact Online",
                                                        # Fixed abbreviated "Intell" to full word
                                                        r"ExecVision Conversation Intelligence",
                                                        r"Exelare",
                                                        r"FileMaker",
                                                        r"fireflies.ai",
                                                        r"FIVE CRM",
                                                        r"Five9 UC Adapter",
                                                        r"Fivetran",
                                                        r"FlexKIDS",
                                                        r"FreeBusy Scheduling Assistant",
                                                        r"Freshdesk",
                                                        r"Gainsight",
                                                        r"Genesis Call Accounting",
                                                        r"GitHub",
                                                        r"GleanView",
                                                        r"GoldMine",
                                                        r"Gong",
                                                        r"Google Contacts",
                                                        r"Google Dialogflow",
                                                        r"Google Drive",
                                                        r"Google Workspace Add-On",
                                                        r"GoSquared",
                                                        r"Halo Service Desk",
                                                        r"Hansen Call",
                                                        r"HappyFox",
                                                        r"HCL Notes",
                                                        r"Heroku",
                                                        r"High Volume SMS",
                                                        r"HubSpot",
                                                        r"Incoming Webhook",
                                                        r"Infor CRM / Saleslogix",
                                                        r"Informacast Fusion",
                                                        r"Insightly",
                                                        r"InsuredMine CRM",
                                                        r"ipTTY",
                                                        r"IVR Orchestrator",
                                                        r"Jenkins",
                                                        r"Jiminny",
                                                        r"Jira",
                                                        r"JobDiva",
                                                        r"KeeperAI",
                                                        r"Kilterly",
                                                        r"Kommo (amoCRM)",
                                                        r"Kommo Text Messaging",
                                                        r"kuando Busylight",
                                                        r"Kustomer",
                                                        r"Lawmatics",
                                                        r"LDAP",
                                                        r"LeadsBridge",
                                                        r"LeadSquared",
                                                        r"Librato",
                                                        r"Liquid Voice",
                                                        r"Logical Office",
                                                        r"Lotus Notes",
                                                        r"LQBKBot",
                                                        r"LTI",
                                                        r"MagicTime",
                                                        r"Mailchimp",
                                                        r"Management System",
                                                        r"Marchex",
                                                        r"Marketo",
                                                        r"Maximizer",
                                                        r"Maximizer CRM",
                                                        r"Meeting Intelligence",
                                                        r"MessageWatcher",
                                                        r"MF Dach",
                                                        r"Microdec",
                                                        r"Microsoft Access",
                                                        r"Microsoft Dynamics 365",
                                                        r"Microsoft Dynamics AX",
                                                        r"Microsoft Dynamics CRM",
                                                        r"Microsoft Dynamics NAV",
                                                        r"Microsoft Exchange Server",
                                                        r"Microsoft Office 2013",
                                                        r"Microsoft Office 2016",
                                                        r"Microsoft Office 365",
                                                        r"Microsoft OneDrive",
                                                        r"Microsoft Outlook",
                                                        r"Microsoft Teams",
                                                        r"Monday.com",
                                                        r"NetSuite",
                                                        r"New Relic",
                                                        r"noCRM.io",
                                                        r"Noise Firewall",
                                                        r"Notifications+",
                                                        r"NowCerts",
                                                        r"Nuclei",
                                                        r"Nutshell",
                                                        r"ODBC",
                                                        r"Okta",
                                                        r"OnContact",
                                                        r"OneLogin",
                                                        r"OpenIQ",
                                                        r"OpsGenie",
                                                        r"Outlook Web Access",
                                                        r"Outplay",
                                                        r"PagerDuty",
                                                        r"Papertrail",
                                                        r"Party Lottery",
                                                        r"PCHomes",
                                                        r"PCRecruiter",
                                                        r"Perfect View",
                                                        r"PHMG",
                                                        r"Pingdom",
                                                        r"Pipedream",
                                                        r"Pipedrive",
                                                        r"Pivotal Tracker",
                                                        r"PlayVox",
                                                        r"Poll Add-In",
                                                        r"Postcall",
                                                        r"PracticeSuite",
                                                        r"Presence Hub",
                                                        r"Prodoscore",
                                                        r"Professional Voice Overs & Audio Messaging Tools",
                                                        r"PunchAlert",
                                                        r"QuickBooks Online",
                                                        r"QV Accelerate",
                                                        r"Rafiki.ai",
                                                        r"Raygun",
                                                        r"RCCP-Free for WordPress",
                                                        r"Re:amaze Live Chat & Helpdesk",
                                                        r"Real-Time Guidance",
                                                        r"ReallySimple Systems",
                                                        r"Redtail CRM",
                                                        r"ReTeam Notifier",
                                                        r"Rezi",
                                                        r"RingClone",
                                                        r"Robin",
                                                        r"RPM",
                                                        r"Runscope",
                                                        r"Sage 50 Accounts",
                                                        r"Sage ACT!",
                                                        r"Sage CRM",
                                                        r"Salesforce CRM",
                                                        r"Salesmate",
                                                        r"SalesNexus",
                                                        r"Salos AutomaaT Go",
                                                        r"Salpo CRM",
                                                        r"SAP Business One",
                                                        r"SAP CRM",
                                                        r"SAP R/3",
                                                        r"Schleupen CS",
                                                        r"Semaphore",
                                                        r"ServiceMax / MobileMax Service",
                                                        r"serviceminder.io",
                                                        r"ServiceNow",
                                                        r"sevDesk",
                                                        r"Shadow Agent",
                                                        r"Shadow All in One Analytics",
                                                        r"Shadow OSN",
                                                        r"Shadow Spaces",
                                                        r"Shopware",
                                                        r"Simple Team Messaging",
                                                        r"Simplicate",
                                                        r"Skype for Business",
                                                        r"Slack",
                                                        r"Smarsh",
                                                        r"Statapile Speech Analytics",
                                                        r"Statflo",
                                                        r"Statuspage.io",
                                                        r"StorageMax",
                                                        r"Sugar CRM",
                                                        r"SumoLogic",
                                                        r"SuperOffice CRM",
                                                        r"Swisscom Directories",
                                                        r"sync.blue",
                                                        r"Team Messaging Auto-Responder",
                                                        r"TeamGram",
                                                        r"Teamleader",
                                                        r"TeamSupport",
                                                        r"TechMan Garage Management System",
                                                        r"Tenfold Integration",
                                                        r"Theta Lake",
                                                        r"TieiT App",
                                                        r"TigerTMS",
                                                        r"Titan",
                                                        r"Travis CI",
                                                        r"Tray Platform",
                                                        r"Trello Bot",
                                                        r"Trumpia Connect",
                                                        r"Twitter Bot",
                                                        r"UC Server",
                                                        r"Userlike",
                                                        r"Velvetech",
                                                        r"Victor Ops",
                                                        r"VMware Phone Plugin",
                                                        r"VoiceSignals",
                                                        r"Voxjar",
                                                        r"Voyager Infinity",
                                                        r"Vtiger",
                                                        r"Vuesion QX Contact Center",
                                                        r"Websolve",
                                                        r"weclapp",
                                                        r"Wice",
                                                        r"WinSIMS",
                                                        r"Workato Automation",
                                                        r"Workato CRM Integrations",
                                                        r"Workbooks",
                                                        r"Workd",
                                                        r"Xentral",
                                                        r"Xima CCaaS",
                                                        r"XSELL Realtime Call Tracking",
                                                        r"Yactraq-Speech Analytics",
                                                        r"YAPI Phone Assistant",
                                                        r"Yellowfin Connector",
                                                        r"Yoobi",
                                                        r"Zammad",
                                                        r"Zapier",
                                                        r"Zendesk",
                                                        r"Zoho CRM",
                                                        r"Zoho Desk",
                                                        r"Andere"
                                                    ]
                                                    Ask_Anzahl._registry[Q].ask_select(
                                                        list(CTI_Liste))
                                                if Fragen_key == "S43":
                                                    Ask_Anzahl._registry[Q].ask_text(
                                                    )

                                            elif Fragen_key != "S28" and Fragen_key != "S29" and Fragen_key != "S30" and Fragen_key != "S31" and Fragen_key != "S32" and Fragen_key != "S33":
                                                Ask_Anzahl._registry[Q].ask_nmb(
                                                )
                                        else:
                                            Ask_Anzahl._registry[Q].ask(
                                            )

    full_awnser_list_S = {}
    full_awnser_list = {}
    name_key_dict = {}

    for Fragen in range(len(Ask_Anzahl._registry)):
        if Ask_Anzahl._registry[Fragen].key != "S20":
            if Ask_Anzahl._registry[Fragen].key != "S12":
                if Ask_Anzahl._registry[Fragen].key != "S13":
                    if Ask_Anzahl._registry[Fragen].key != "S57":
                        if Ask_Anzahl._registry[Fragen].key != "S11":
                            if Ask_Anzahl._registry[Fragen].key != "S58":
                                if Ask_Anzahl._registry[Fragen].key != "S14":
                                    if Ask_Anzahl._registry[Fragen].key != "S10":
                                        if Ask_Anzahl._registry[Fragen].key != "S16":
                                            if st.session_state.current_role != "veovia":
                                                if Ask_Anzahl._registry[Fragen].key != "Kzoho":
                                                    full_awnser_list_S[Ask_Anzahl._registry[Fragen]
                                                                       .key] = st.session_state[Ask_Anzahl._registry[Fragen].key]
                                                    name_key_dict[Ask_Anzahl._registry[Fragen]
                                                                  .name] = Ask_Anzahl._registry[Fragen].key
                                            else:
                                                if Ask_Anzahl._registry[Fragen].key != "KundenName":
                                                    full_awnser_list_S[Ask_Anzahl._registry[Fragen]
                                                                       .key] = st.session_state[Ask_Anzahl._registry[Fragen].key]
                                                    name_key_dict[Ask_Anzahl._registry[Fragen]
                                                                  .name] = Ask_Anzahl._registry[Fragen].key

    for Fragen in range(len(Ask_Question._registry)):

        full_awnser_list[Ask_Question._registry[Fragen]
                         .key] = st.session_state[Ask_Question._registry[Fragen].key]
        name_key_dict[Ask_Question._registry[Fragen]
                      .name] = Ask_Question._registry[Fragen].key

    return full_awnser_list, full_awnser_list_S, name_key_dict


@st.fragment
def ask_kunden_daten_with_key_name_return():
    import datetime

    def last_day_of_month(any_day):
        # The day 28 exists in every month. 4 days later, it's always next month
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
        # subtracting the number of the current day brings us back one month
        return next_month - datetime.timedelta(days=next_month.day)
    today = datetime.datetime.now().date() + relativedelta(months=1)
    kunden_daten = {}
    key_name_dict = {}
    lead_quellen_optionen = ["Alt-Bestand", "Bestand Home", "Avaya Neu", "Avaya-Bestand", "Avaya.de Abkündigung", "bedirekt", "Deutsche Glasfaser", "DHO", "Digital Phone", "DNS:NET", "DNS:NET Partner", "DTS-Systems Partner", "Google Plus", "Hunter Solution", "Inbound Call", "Live-Akquise", "Kundenempfehlung", "LinkedIn",
                             "marketingmanufaktur", "Messe", "MHWK", "MVF", "NFON", "NFON-Partner", "NFON-Direct-Sales", "persönlicher Kontakt", "Placatel", "RLA", "Telefonica", "Telemarketing", "TK-Vergleich", "Trading Twins", "Vodafone Direct Sales", "VVL", "Webformular", "WHBTA", "Zusatzgeschäft", "And Friends", "ASC-MV", "WTG", "NFON Ablöse"]

    kunden_daten["lead_quelle"] = Ask_Anzahl(
        "K2", "Leadquelle", help="").ask_select(lead_quellen_optionen)

    kunden_daten["abschluss_datum"] = Ask_Anzahl(
        "K3", "Voraussichtliches Abschlussdatum", help="").ask_date(early_date=last_day_of_month(today))

    early_date = kunden_daten.get(
        'abschluss_datum') + relativedelta(months=2, day=1)

    kunden_daten["start_datum"] = Ask_Anzahl(
        "K4", "Startdatum", help="").ask_date(early_date)

    if kunden_daten['start_datum'] < early_date:
        st.markdown(
            ":red[**Bitte Start - und Abschluss-Datum Anpassen, Startdatum muss mindestens 1 Monat nach Abschluss sein !**]")
    liste_grund = [
        "Erstanschaffung", "Aktuelle Anlage defekt", "Homeoffice-Funktionen benötigt", "IT-Systeme integrieren", "Alter aktuelle Anlage", "Andere"]
    kunden_daten['grund'] = []

    st.write("**Grund für die neue Telefonanlage**")
    for item in range(len(liste_grund)):
        nbm = item + 5
        _key = "K" + str(nbm)

        Ask_Anzahl(key=_key, name=liste_grund[item], help="").ask_checkbox()

        kunden_daten['grund'].append(st.session_state[_key])
        key_name_dict[f'Grund für die neue Telefonanlage:{liste_grund[item]}'] = _key

    kunden_daten['grund'] = Ask_Anzahl(
        key="K11", name="Genauere Beschreibung", help="").ask_text()

    key_name_dict["Kontaktname"] = "K1"
    key_name_dict["Leadquelle"] = "K2"
    key_name_dict["Abschlussdatum(voraussichtlich)"] = "K3"
    key_name_dict["Startdatum"] = "K4"
    return kunden_daten, key_name_dict
