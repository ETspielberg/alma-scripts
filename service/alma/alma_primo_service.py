import logging



# base ALMA API url
import os

# basic url parameters for the Primo VE API
import requests

primo_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/primo/'
vid = '49HBZ_UDE:49HBZ_UDE_default_view'
tab = 'LibraryCatalog'
scope = 'MyInstitution'
lang = 'eng'
sort = 'rank'
pcAvailability = 'true'
institution_code = '49HBZ_UDE'
alma_domain_name = 'hbz-ubdue'
getMore = '0'
skipDelivery = 'false'
conVoc = 'true'
disableSplitFacets = 'true'

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'

# API-Key aus den Umgebungsvariablen lesen
primo_api_key = os.environ['PRIMO_SCRIPT_API_KEY']

# generate general parameters string (missing API-key, limit, offset and query)
base_parameter = 'vid={}&tab={}&scope={}&lang={}&sort={}&pcAvailability={}&getMore={}&conVoc={}&inst={}&skipDelivery={}&disableSplitFacets={}' \
    .format(vid, tab, scope, lang, sort, pcAvailability, getMore, conVoc, institution_code, skipDelivery, disableSplitFacets)


def collect_portfolios_and_packages(project, field, search_term, number_of_hits):
    query = '{}%2Ccontains%2C{}'.format(field, search_term)
    logging.debug('running query: {}'.format(query))

    # Start-Werte für den offset und die Ergebnisanzahl pro Seite setzen
    offset = 0
    limit = 50

    # Listen initialisieren

    portfolios = []
    packages = []

    # Alle Daten einsammeln
    while offset < number_of_hits:
        logging.info('project {}, entry {}: collected {} entries out of {}'.format(project, search_term, str(len(portfolios) + len(packages)), number_of_hits))
        # Die URL für die API zusammensetzen
        url = '{}v1/search?{}&q={}&offset={}&limit={}&apikey={}' \
            .format(primo_api_base_url, base_parameter, query, offset, limit, primo_api_key)

        # Die GET-Abfrage ausführen
        get_list = requests.get(url=url, headers={'Accept': 'application/json'})

        # Die Kodierung der Antwort auf UTF-8 festlegen
        get_list.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get_list.status_code == 200:
            for doc in get_list.json()['docs']:
                # Typ und MMS-ID auslesen
                doc_type = doc['pnx']['display']['type'][0]
                mms = doc['pnx']['display']['mms'][0]

                # in die entsprechende Liste einsortieren
                if doc_type == 'database':
                    packages.append(mms)
                else:
                    portfolios.append(mms)
        else:
            logging.error(get_list.text)
            break
        offset = offset + limit
    return portfolios, packages
