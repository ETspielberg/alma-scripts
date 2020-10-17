import logging
import os

import requests

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'
alma_api_key = os.environ['ALMA_SCRIPT_API_KEY']


def retrieve_collection_id_for_mms_id(project, mms_ids):
    collection_ids = []

    # Alle MMS-IDs durchgehen
    for mms_id in mms_ids.split(';'):

        logging.info('project {}: reading information for e-collection {}'.format(project, mms_id))

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

            try:
                for collection in info['electronic_collection']:
                    collection_ids.append(collection['id'])
            except KeyError:
                logging.error('project {}: no collection found for mms_id {}'.format(project, mms_id))

    return list(dict.fromkeys(collection_ids))


def retrieve_portfolio_ids(project, mms_ids):
    portfolio_ids = []
    # Alle MMS-IDs durchgehen
    for mms_id in mms_ids.split(';'):

        logging.info('project {}: reading Information for portfolio {}'.format(project, mms_id))

        # Die URL für die API zusammensetzen
        url = '{}bibs/{}/portfolios?apikey={}'.format(alma_api_base_url, mms_id, alma_api_key)

        # GET-Abfrage ausführen
        get = requests.get(url=url, headers={'Accept': 'application/json'})

        # Kodierung auf UTF-8 festsetzen
        get.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get.status_code == 200:

            # Den Inhalt der Antwort als Text auslesen
            info = get.json()

            try:
                for collection in info['portfolio']:
                    portfolio_ids.append(collection['id'])
            except KeyError:
                logging.error('project {}: no portfolio found for mms_id {}'.format(project, mms_id))

    collection_ids = list(dict.fromkeys(portfolio_ids))