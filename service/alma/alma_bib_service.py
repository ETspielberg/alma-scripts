import logging
import os

import requests

from service.alma import alma_electronic_service

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'
alma_api_key = os.environ['ALMA_SCRIPT_API_KEY']


def retrieve_collection_id_for_mms_id(project, mms_ids, library=None):
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
                    collection_id = collection['id']
                    if library is None:
                        collection_ids.append(collection_id)
                    else:
                        collection = alma_electronic_service.get_collection(collection_id=collection_id)
                        try:
                            if collection['library']['value'] == library:
                                collection_ids.append(collection_id)
                        except:
                            logging.warning('project {}: could not retrieve collection {}'.format(project, collection_id))
            except KeyError:
                logging.error('project {}: no collection found for mms_id {}'.format(project, mms_id))
    return list(dict.fromkeys(collection_ids))


def retrieve_portfolio_ids(project, mms_ids, library=None):
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
                for portfolio in info['portfolio']:
                    portfolio_id = portfolio['id']
                    if library is None:
                        portfolio_ids.append(portfolio_id)
                    else:
                        portfolio_json = alma_electronic_service.get_stand_alone_portfolio(portfolio_id=portfolio_id)
                        try:
                            if portfolio_json['library']['value'] == library:
                                portfolio_ids.append(portfolio_id)
                        except:
                            logging.error('project {}: could not retrieve portfolio {}'.format(project, portfolio_id))
            except KeyError:
                logging.error('project {}: no portfolio found for mms_id {}'.format(project, mms_id))

    return list(dict.fromkeys(portfolio_ids))
