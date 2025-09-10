# lib/impressum.py
import streamlit as st
from urllib.parse import urlencode

# -------- utils --------
def _url_with(overrides: dict) -> str:
    """
    Construit une URL relative en conservant les query params existants.
    - Si après override il ne reste AUCUN paramètre → renvoie './' (même page, sans query).
    - Sinon → renvoie '?...' normal.
    """
    qp = dict(st.query_params)
    for k, v in overrides.items():
        if v is None:
            qp.pop(k, None)
        else:
            qp[k] = v

    qs = urlencode(qp, doseq=True)
    return ("?" + qs) if qs else "./"

# -------- footer links (à afficher en bas des pages où tu veux montrer les 2 liens) --------
def render_footer_links():
    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.link_button("Impressum", _url_with({"view": "impressum"}), use_container_width=True)
    with c2:
        st.link_button("Datenschutz", _url_with({"view": "datenschutz"}), use_container_width=True)

# -------- pages légales --------
def render_legal_page(view: str):
    # Barre d’actions : uniquement un bouton de retour “sûr”
    st.link_button("← Zurück", _url_with({"view": None}))

    if view == "impressum":
        st.title("Impressum")
        st.markdown(
            """
**7WERK media GmbH**  
Leunaer Str. 7  
D-12681 Berlin

**AG Berlin (Charlottenburg)** HRB 176649 B  
**Geschäftsführer:** Rainer Mayerhofer

**USt-ID:** DE306351782

**Haftungshinweis:** Trotz sorgfältiger inhaltlicher Kontrolle übernehmen wir keine Haftung für die Inhalte externer Links.  
Für den Inhalt der verlinkten Seiten sind ausschließlich deren Betreiber verantwortlich.
            """
        )
        return

    # ------------------ DATENSCHUTZERKLÄRUNG ------------------
    st.title("Datenschutzerklärung")
    st.header("Allgemeiner Hinweis und Pflichtinformationen")

    st.subheader("Benennung der verantwortlichen Stelle")
    st.markdown(
        """
Die verantwortliche Stelle für die Datenverarbeitung auf dieser Website ist:

**Rechtsanwalt Ronald Hoyer**  
Leunaer Straße 7  
12681 Berlin  
datenschutz@7werk.cloud

Die verantwortliche Stelle entscheidet allein oder gemeinsam mit anderen über die Zwecke und Mittel der Verarbeitung von personenbezogenen Daten (z. B. Namen, Kontaktdaten o. Ä.).
        """
    )

    st.subheader("Widerruf Ihrer Einwilligung zur Datenverarbeitung")
    st.markdown(
        """
Nur mit Ihrer ausdrücklichen Einwilligung sind einige Vorgänge der Datenverarbeitung möglich. Ein Widerruf Ihrer bereits erteilten Einwilligung ist jederzeit möglich. Für den Widerruf genügt eine formlose Mitteilung per E-Mail. Die Rechtmäßigkeit der bis zum Widerruf erfolgten Datenverarbeitung bleibt vom Widerruf unberührt.
        """
    )

    st.subheader("Recht auf Beschwerde bei der zuständigen Aufsichtsbehörde")
    st.markdown(
        """
Als Betroffener steht Ihnen im Falle eines datenschutzrechtlichen Verstoßes ein Beschwerderecht bei der zuständigen Aufsichtsbehörde zu. Zuständige Aufsichtsbehörde bezüglich datenschutzrechtlicher Fragen ist der Landesdatenschutzbeauftragte des Bundeslandes, in dem sich der Sitz unseres Unternehmens befindet. Der folgende Link stellt eine Liste der Datenschutzbeauftragten sowie deren Kontaktdaten bereit:  
<https://www.bfdi.bund.de/DE/Infothek/Anschriften_Links/anschriften_links-node.html>.
        """
    )

    st.subheader("Recht auf Datenübertragbarkeit")
    st.markdown(
        """
Ihnen steht das Recht zu, Daten, die wir auf Grundlage Ihrer Einwilligung oder in Erfüllung eines Vertrags automatisiert verarbeiten, an sich oder an Dritte aushändigen zu lassen. Die Bereitstellung erfolgt in einem maschinenlesbaren Format. Sofern Sie die direkte Übertragung der Daten an einen anderen Verantwortlichen verlangen, erfolgt dies nur, soweit es technisch machbar ist.
        """
    )

    st.subheader("Recht auf Auskunft, Berichtigung, Sperrung, Löschung")
    st.markdown(
        """
Sie haben jederzeit im Rahmen der geltenden gesetzlichen Bestimmungen das Recht auf unentgeltliche Auskunft über Ihre gespeicherten personenbezogenen Daten, Herkunft der Daten, deren Empfänger und den Zweck der Datenverarbeitung und ggf. ein Recht auf Berichtigung, Sperrung oder Löschung dieser Daten. Diesbezüglich und auch zu weiteren Fragen zum Thema personenbezogene Daten können Sie sich jederzeit über die im Impressum aufgeführten Kontaktmöglichkeiten an uns wenden.
        """
    )

    st.subheader("SSL- bzw. TLS-Verschlüsselung")
    st.markdown(
        """
Aus Sicherheitsgründen und zum Schutz der Übertragung vertraulicher Inhalte, die Sie an uns als Seitenbetreiber senden, nutzt unsere Website eine SSL- bzw. TLS-Verschlüsselung. Damit sind Daten, die Sie über diese Website übermitteln, für Dritte nicht mitlesbar. Sie erkennen eine verschlüsselte Verbindung an der „https://“-Adresszeile Ihres Browsers und am Schloss-Symbol in der Browserzeile.
        """
    )

    st.subheader("Server-Log-Dateien")
    st.markdown(
        """
In Server-Log-Dateien erhebt und speichert der Provider der Website automatisch Informationen, die Ihr Browser automatisch an uns übermittelt. Dies sind:

- Besuchte Seite auf unserer Domain  
- Datum und Uhrzeit der Serveranfrage  
- Browsertyp und Browserversion  
- Verwendetes Betriebssystem  
- Referrer URL  
- Hostname des zugreifenden Rechners  
- IP-Adresse

Es findet keine Zusammenführung dieser Daten mit anderen Datenquellen statt. Grundlage der Datenverarbeitung bildet **Art. 6 Abs. 1 lit. b DSGVO**, der die Verarbeitung von Daten zur Erfüllung eines Vertrags oder vorvertraglicher Maßnahmen gestattet.

*Quelle: Datenschutz-Konfigurator von Mein-Datenschutzbeauftragter.de*
        """
    )
