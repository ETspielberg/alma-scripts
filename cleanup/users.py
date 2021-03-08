import json
import logging
import os
import time

import requests

from service.list_reader_service import load_identifier_list_of_type
from service.table_reader_service import read_requests_table

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'

def update_users(project):
    # Datei mit den Benutzerkennungen laden
    users = load_identifier_list_of_type(project)

    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Nutzer durchgehen
    for user in users:

        # Die URL für die API zusammensetzen
        url = '{}users/{}?apikey={}'.format(alma_api_base_url, user, api_key)

        # GET-Abfrage ausführen
        get = requests.get(url=url, headers={'Accept': 'application/json'})

        # Kodierung auf UTF-8 festsetzen
        get.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get.status_code == 200:

            # Den Inhalt der Antwort als Text auslesen
            info = get.text

            # Prüfen, ob der Nutzer Public ist
            if '"value":"PUBLIC","desc":"Public"' in info:

                # In dem Text den Typ auf Staff ändern
                payload = info.replace('"record_type":{"value":"PUBLIC","desc":"Public"}',
                                       '"record_type":{"value":"STAFF","desc":"Staff"}')

                # Update als PUT-Abfrage ausführen. URL ist die gleiche, Encoding ist wieder utf-8, Inhalt ist JSON
                update = requests.put(url=url, data=payload.encode('utf-8'),
                                      headers={'Content-Type': 'application/json'})

                # Antwort-Encoding ist wieder UTF-8
                update.encoding = 'utf-8'

                # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
                if update.status_code == 200:
                    logging.info('succesfully updated user {}'.format(user))
                else:
                    logging.warning('problem updating user {}:{}'.format(user, update.text))
            else:
                logging.info('user {} not public'.format(user))
        else:
            logging.error(get.text)




def set_requests():
    requests_table = read_requests_table()
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for index, request in requests_table.iterrows():
        time.sleep(0.5)
        barcode = request['Barcode'].strip()
        user_id = request['Benutzer_id'].strip()
        try:
            logging.debug('processing item with barcode {}'.format(barcode))
            url = '{}items?item_barcode={}&apikey={}'.format(alma_api_base_url, barcode, api_key)
            response = requests.get(url=url, headers={'Accept': 'application/json'})

            date = request['angelegt am'].strip()
            # Die Kodierung der Antwort auf UTF-8 festlegen
            response.encoding = 'utf-8'

            # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
            if response.status_code == 200:
                response_json = response.json()
                mms_id = response_json['bib_data']['mms_id']
                holding_id = response_json['holding_data']['holding_id']
                item_id = response_json['item_data']['pid']
                requests_url = '{}users/{}/requests?mms_id={}&apikey={}'.format(alma_api_base_url, user_id, mms_id,
                                                                                api_key)
                request_json = {}
                request_json['request_type'] = 'HOLD'
                request_json['holding_id'] = holding_id
                request_json['pickup_location_type'] = 'LIBRARY'
                request_json['pickup_location_library'] = request['Abholort'].strip()
                request_json['booking_start_date'] = '{}-{}-{}'.format(date[:4], date[4:6], date[6:])
                set_request = requests.post(url=requests_url, data=json.dumps(request_json).encode('utf-8'),
                                            headers={'Content-Type': 'application/json'})

                if set_request.status_code == 200:
                    logging.info(
                        'succesfully created request for user {} for mms_id, holding_id {}, {}'.format(user_id, mms_id,
                                                                                                       holding_id))
                else:
                    logging.error(
                        'problem creating request for user {} for mms_id, holding_id {}, {}'.format(user_id, mms_id,
                                                                                                    holding_id))
                    logging.error(set_request.status_code)
                    logging.error(set_request.text)

            else:
                logging.warning(response.text)
        except:
            logging.error(
                'problem creating request for user {} and item - no response from API'.format(user_id, item_id))

