import json
import logging
import os

import requests

from service.table_reader_service import read_table, write_table

primo_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/primo/'
vid = '49HBZ_UDE:49HBZ_UDE_default_view'
tab = 'LibraryCatalog'
scope = 'MyInstitution'
lang = 'eng'
sort = 'rank'
pcAvailability = 'true'
inst = '49HBZ_UDE'
getMore = '0'
skipDelivery = 'false'
conVoc = 'true'
disableSplitFacets = 'true'
base_parameter = 'vid={}&tab={}&scope={}&lang={}&sort={}&pcAvailability={}&getMore={}&conVoc={}&inst={}&skipDelivery={}&disableSplitFacets={}' \
    .format(vid, tab, scope, lang, sort, pcAvailability, getMore, conVoc, inst, skipDelivery, disableSplitFacets)

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'

# API-Key aus den Umgebungsvariablen lesen
primo_api_key = os.environ['PRIMO_SCRIPT_API_KEY']
alma_api_key = os.environ['ALMA_SCRIPT_API_KEY']


def collect_number_of_entries(project):
    logging.info('project {}: starting collection of total number of entries'.format(project))
    # Excel-Tabelle mit Abrufkennzeichen einlesen
    table = read_table(project)

    # Neue Tabelle vorbereiten
    new_table_rows = []

    for index, row in table.iterrows():
        logging.info('processing mark {}'.format(row['Kennzeichen']))
        query = 'lds03%2Ccontains%2C{}'.format(row['Kennzeichen'])
        logging.debug('running query: {}'.format(query))
        offset = 0
        limit = 1
        # Die URL für die API zusammensetzen
        url = '{}v1/search?{}&q={}&offset={}&limit={}&apikey={}' \
            .format(primo_api_base_url, base_parameter, query, offset, limit, primo_api_key)
        logging.debug('querying url {}'.format(url))

        # Die GET-Abfrage ausführen
        get_list = requests.get(url=url, headers={'Accept': 'application/json'})

        # Die Kodierung der Antwort auf UTF-8 festlegen
        get_list.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get_list.status_code == 200:
            # Gesamtzahl der gefundenen Einträge abrufen
            try:
                total_number_of_results = get_list.json()['info']['total']
            except KeyError:
                logging.error('error message received')
                total_number_of_results = 0
        else:
            total_number_of_results = 0
            logging.error(get_list.text)
        row['total_in_Alma'] = total_number_of_results
        new_table_rows.append(row)
    return write_table(project, new_table_rows)


def collect_mms_ids(project, table):
    logging.info('project {}: starting collection of mms ids'.format(project))
    new_table_rows = []
    for index, row in table.iterrows():
        # Suche initialisieren
        query = 'lds03%2Ccontains%2C{}'.format(row['Kennzeichen'])
        logging.debug('running query: {}'.format(query))

        # Start-Werte für den offset und die Ergebnisanzahl pro Seite setzen
        offset = 0
        limit = 50

        # Listen initialisieren

        portfolios = []
        packages = []

        # Alle Daten einsammeln
        while len(portfolios) + len(packages) < row['total_in_Alma']:
            logging.info('project {}, entry {}: collected {} entries out of {}'.format(project, row['Kennzeichen'], str(
                len(portfolios) + len(packages)), row['total_in_Alma']))
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
        row['Paket_MMS'] = list_to_string(packages)
        row['Portfolio_MMS'] = list_to_string(portfolios)
        new_table_rows.append(row)

    return write_table(project, new_table_rows)


def list_to_string(mms_list):
    if len(mms_list) == 0:
        return ''
    else:
        string = mms_list.pop()
        while len(mms_list) > 0:
            string = string + ';' + mms_list.pop()
        return string


def retrieve_collection_ids(project, table):
    new_table_rows = []

    # Alle Pakete durchgehen
    for index, row in table.iterrows():

        collection_ids = []

        # Alle MMS-IDs durchgehen
        for mms_id in row['Paket_MMS'].split(';'):

            logging.info('reading Information for package {}'.format(mms_id))

            # Die URL für die API zusammensetzen
            url = '{}bibs/{}/e-collections?apikey={}'.format(alma_api_base_url, mms_id, alma_api_key)

            # GET-Abfrage ausführen
            get = requests.get(url=url, headers={'Accept': 'application/json'})

            # Kodierung auf UTF-8 festsetzen
            get.encoding = 'utf-8'

            # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
            if get.status_code == 200:

                # Den Inhalt der Antwort als Text auslesen
                info = get.json()

                for collection in info['electronic_collection']:
                    collection_ids.append(collection['id'])

        collection_ids = list(dict.fromkeys(collection_ids))
        row['Collection_IDs'] = list_to_string(collection_ids)
        new_table_rows.append(row)
    return write_table(project, new_table_rows)


def update_collections(table):
    # Alle Pakete durchgehen
    new_table_rows = []
    for index, row in table.iterrows():
        service_ids = []
        # Alle Collection-IDs durchgehen
        for collection_id in row['Collection_IDs'].split(';'):
            logging.info('updating collection {}'.format(collection_id))

            # Den Type auf selective package ändern
            success_type_change = set_type_to_selectiove_package(collection_id)

            # Falls die Änderung erfolgreich war, einen Full-Text-Service hinzufügen
            if success_type_change:
                service_ids = add_active_service(collection_id)
        row['Service_IDs'] = list_to_string(service_ids)
        new_table_rows.append(row)
    return write_table(project, new_table_rows)


def set_type_to_selectiove_package(collection_id):
    # Die URL für die API zusammensetzen
    url = '{}electronic/e-collections/{}?apikey={}'.format(alma_api_base_url, collection_id, alma_api_key)

    # GET-Abfrage ausführen
    get = requests.get(url=url, headers={'Accept': 'application/json'})

    # Kodierung auf UTF-8 festsetzen
    get.encoding = 'utf-8'

    # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
    if get.status_code == 200:

        # Die Antwort als Text auslesen
        info = get.text

        # den Typ-Wert "Database" durch "Selective package" ersetzen
        payload = info.replace('"type":{"value":"6","desc":"Database"}',
                               '"type":{"value":"0","desc":"Selective package"}')

        # Update als PUT-Abfrage ausführen. URL ist die gleiche, Encoding ist wieder utf-8, Inhalt ist JSON
        update = requests.put(url=url, data=payload.encode('utf-8'),
                              headers={'Content-Type': 'application/json'})

        # Die Kodierung der Antwort auf UTF-8 festlegen
        update.encoding = 'utf-8'

        # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
        if update.status_code == 200:
            logging.info('succesfully updated e-collection {} to selective package'.format(collection_id))
            return True
        else:
            logging.error('problem updating e-collection {}:{}'.format(collection_id, update.text))
            return False
    else:
        logging.error('could not retrieve collection {}:{}'.format(collection_id, get.text))
        return False


def add_active_service(collection_id):
    logging.info('adding service to collection {}'.format(collection_id))
    # Der Service als Dictionary
    payload = {"is_local": True,
               "type": {
                   "value": "getFullTxt",
                   "desc": "Full Text"
               },
               "public_description": "",
               "public_description_override": "",
               "activation_status": {
                   "value": "11",
                   "desc": "Available"
               },
               "parser": None,
               "free": None,
               "proxy": None
               }

    # die URL zum Hinzufügen eines Service
    url = '{}electronic/e-collections/{}/e-services?apikey={}'.format(alma_api_base_url, collection_id, alma_api_key)

    # POST ausführen.
    post_service = requests.post(url=url, data=json.dumps(payload).encode('utf-8'),
                                 headers={'Content-Type': 'application/json', 'Accept': 'application/json'})

    # Die Kodierung der Antwort auf UTF-8 festlegen
    post_service.encoding = 'utf-8'

    # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
    if post_service.status_code == 200:
        logging.info('successfully added service full text to e-collection {}'.format(collection_id))
        return post_service.json()['id']
    else:
        logging.error('problems adding service full text to e-collection {}:{}'.format(collection_id, post_service.text))
        return ''

def build_collections(table):
    # Alle Pakete durchgehen
    for index, row in table.iterrows():

        for mms_id in row['Portfolio_MMS'].split(';'):
            service_id = row['Service_IDs'][0]
            collection_id = row['Collection_IDs'][0]
            add_portfolios_to_collection(mms_id, service_id, collection_id)


def add_portfolios_to_collection(mms_id, service_id, collection_id):
    # Die URL für die API zusammensetzen
    url = '{}bibs/{}/portfolios'.format(alma_api_base_url, mms_id)

    # GET-Abfrage ausführen
    get = requests.get(url=url, headers={'Accept': 'application/json'})

    # Kodierung auf UTF-8 festsetzen
    get.encoding = 'utf-8'

    # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
    if get.status_code == 200:

        # Antwort als JSON auslesen
        info = get.json()

        # Portfolios zur Kollektion hinzufügen
        for portfolio in info['portfolio']:

            # URL für API generieren
            url = '{}electronic/e-collections/{}/e-services/{}/portfolios?apikey={}'.format(alma_api_base_url, collection_id, service_id, alma_api_key)

            # POST ausführen.
            post_portfolio = requests.post(url=url, data=json.dumps(portfolio).encode('utf-8'),
                                 headers={'Content-Type': 'application/json', 'Accept': 'application/json'})

            # Die Kodierung der Antwort auf UTF-8 festlegen
            post_portfolio.encoding = 'utf-8'

            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if post_portfolio.status_code == 200:
                logging.info('successfully added portfolio {} to service {} of e-collection {}'.format(portfolio['id'], service_id, collection_id))
                return True
            else:
                logging.error(
                    'problems adding ortfolio {} to service {} of e-collection {}:{}'.format(portfolio['id'], service_id, collection_id, post_portfolio.text))
                return False
    else:
        return False




if __name__ == '__main__':
    # den Namen des Laufs angeben. Dieser definiert den name der Log-Datei und den Typ an Liste, die geladen wird.
    project = 'marks'

    # den Namen der Logdatei festlegen
    log_file = 'data/output/{}.log'.format(project)

    # den Logger konfigurieren (Dateinamen, Level)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=log_file, level=logging.INFO)

    # die Gesamtzahl der Einträge abrufen
    table = collect_number_of_entries(project)

    # die MMS-IDs hinzuschreiben
    # table = collect_mms_ids(project, table)

    # Die Collection-IDs der e-Kollektionen heraussammeln
    # table = retrieve_collection_ids(project, table)

    # table = update_collections(table)
