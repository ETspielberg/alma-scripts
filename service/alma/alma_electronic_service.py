import json
import logging
import os

import requests

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'
alma_api_key = os.environ['ALMA_SCRIPT_API_KEY']


def get_number_of_portfolios(mms_id):
    url = '{}bib/{}/portfolios?apikey={}'.format(alma_api_base_url, mms_id, alma_api_key)

    # GET-Abfrage ausführen
    get = requests.get(url=url, headers={'Accept': 'application/json'})

    # Kodierung auf UTF-8 festsetzen
    get.encoding = 'utf-8'

    # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
    if get.status_code == 200:
        # Den Inhalt der Antwort als Text auslesen
        return get.json()['total_record_count']
    elif get.status_code == 404:
        return 0
    else:
        logging.warning('could not retrieve number of portfolios. got error {}: {}'.format(get.status_code, get.text))
        return 0


def retrieve_service_ids(project, collection_ids):
    service_ids = []
    for collection_id in collection_ids:

        logging.info('project {}: reading Information for collection {}'.format(project, collection_id))

        # Die URL für die API zusammensetzen
        url = '{}electronic/{}/e-collections?apikey={}'.format(alma_api_base_url, collection_id, alma_api_key)

        # GET-Abfrage ausführen
        get = requests.get(url=url, headers={'Accept': 'application/json'})

        # Kodierung auf UTF-8 festsetzen
        get.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get.status_code == 200:

            # Den Inhalt der Antwort als Text auslesen
            info = get.json()

            try:
                for collection in info['e-services']:
                    service_ids.append(collection['id'])
            except KeyError:
                logging.error('project {}: no portfolio found for collection_id {}'.format(project, collection_id))

    return list(dict.fromkeys(service_ids))


def retrieve_services(collection_id):
    url = '{}electronic/{}/e-collections/e-services?apikey={}'.format(alma_api_base_url, collection_id, alma_api_key)

    # GET-Abfrage ausführen
    get = requests.get(url=url, headers={'Accept': 'application/json'})

    # Kodierung auf UTF-8 festsetzen
    get.encoding = 'utf-8'

    # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
    if get.status_code == 200:

        # Den Inhalt der Antwort als Text auslesen
        info = get.json()


def get_collection(collection_id):
    url = '{}electronics/e-collections/{}?apikey={}'.format(alma_api_base_url, collection_id, alma_api_key)
    get_collection = requests.get(url=url, headers={'Accept': 'application/json'})
    if get_collection.status_code == 200:
        return get_collection.json()
    else:
        return None


def get_stand_alone_portfolio(portfolio_id):
    url = '{}electronics/e-collections/{}/e-services/{}/portfolios/{}?apikey={}'.format(alma_api_base_url, portfolio_id, portfolio_id, portfolio_id, alma_api_key)
    get_portfolio = requests.get(url=url, headers={'Accept': 'application/json'})
    if get_portfolio.status_code == 200:
        return get_portfolio.json()
    else:
        return None


# Den Inhalt der Antwort als Text auslesen

def set_type_to_selective_package(project, collection_id):
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
            logging.info(
                'project {}: succesfully updated e-collection {} to selective package'.format(project, collection_id))
            return True
        else:
            logging.error('project {}: problem updating e-collection {}:{}'.format(project, collection_id, update.text))
            return False
    else:
        logging.error('project {}: could not retrieve collection {}:{}'.format(project, collection_id, get.text))
        return False


def add_active_full_text_service(project, collection_id):
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
        logging.info(
            'project {}: successfully added service full text to e-collection {}'.format(project, collection_id))
        return post_service.json()
    else:
        logging.error(
            'project {}: problems adding service full text to e-collection {}:{}'.format(project, collection_id,
                                                                                         post_service.text))
        return ''


def add_portfolios_to_collection(project, portfolio_id, service_id, collection_id):
    # Die URL für die API zusammensetzen
    url = '{}electronic/e-collections/{}/e-services/{}/portfolios?apikey={}'.format(alma_api_base_url, portfolio_id,
                                                                                    portfolio_id, portfolio_id,
                                                                                    alma_api_key)

    # GET-Abfrage ausführen
    get = requests.get(url=url, headers={'Accept': 'application/json'})

    # Kodierung auf UTF-8 festsetzen
    get.encoding = 'utf-8'

    # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
    if get.status_code == 200:

        # Antwort als JSON auslesen
        info = get.json()

        # Portfolios zur Kollektion hinzufügen

        if info['library']['value'] == 'E0001':
            # URL für API generieren
            url = '{}electronic/e-collections/{}/e-services/{}/portfolios?apikey={}'.format(alma_api_base_url,
                                                                                            collection_id, service_id,
                                                                                            alma_api_key)

            # POST ausführen.
            post_portfolio = requests.post(url=url, data=json.dumps(info).encode('utf-8'),
                                           headers={'Content-Type': 'application/json', 'Accept': 'application/json'})

            # Die Kodierung der Antwort auf UTF-8 festlegen
            post_portfolio.encoding = 'utf-8'

            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if post_portfolio.status_code == 200:
                logging.info(
                    'project {}: successfully added portfolio {} to service {} of e-collection {}'.format(project,
                                                                                                          info['id'],
                                                                                                          service_id,
                                                                                                          collection_id))
                return True
            else:
                logging.error(
                    'project {}: problems adding portfolio {} to service {} of e-collection {}:{}'.format(project,
                                                                                                          info['id'],
                                                                                                          service_id,
                                                                                                          collection_id,
                                                                                                          post_portfolio.text))
                return False
    else:
        return False
