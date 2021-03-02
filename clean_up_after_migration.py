import json
import logging
import os
import time

import requests

# lädt die Funktion load_identifier_list_of_type aus der Datei service/list_reader_service.py
from service.google_service import get_google_data
from service.list_reader_service import load_identifier_list_of_type

# Basis-URL für die Alma API
from service.table_reader_service import read_requests_table

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def update_users():
    # Datei mit den Benutzerkennungen laden
    users = load_identifier_list_of_type('users')

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
                update = requests.put(url=url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'})

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


def update_vendors(type):
    # Datei mit den Lieferanten eines Typs laden
    vendors = load_identifier_list_of_type('vendor_' + type)

    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for vendor_line in vendors:

        # Die URL für die API zusammensetzen
        url = '{}acq/vendors/{}?apikey={}'.format(alma_api_base_url, vendor_line, api_key)

        # GET-Abfrage ausführen
        get = requests.get(url=url, headers={'Accept': 'application/json'})

        # Kodierung auf UTF-8 festsetzen
        get.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get.status_code == 200:

            # Den Inhalt der Antwort als Text auslesen
            info = get.text

            # In dem Text den Typ auf Inactive ändern
            payload = info.replace('"status":{"value":"ACTIVE","desc":"Active"}',
                                   '"status":{"value":"INACTIVE","desc":"Inactive"}')

            # Update als PUT-Abfrage ausführen. URL ist die gleiche, Encoding ist wieder utf-8, Inhalt ist JSON
            update = requests.put(url=url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'})

            # Antwort-Encoding ist wieder UTF-8
            update.encoding = 'utf-8'

            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update.status_code == 200:
                logging.info('succesfully updated vendor {}'.format(vendor_line))
            else:
                logging.warning('problem updating vendor {}:{}'.format(vendor_line, update.text))
        else:
            logging.error(get.text)


def deactivate_non_used_vendors(type):
    # Datei mit den Lieferanten eines Typs laden
    vendors = load_identifier_list_of_type('vendor_' + type)

    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for vendor_line in vendors:

        # Die URL für die API zusammensetzen
        url = '{}acq/vendors/{}/po-lines?apikey={}'.format(alma_api_base_url,vendor_line, api_key)

        # Die GET-Abfrage ausführen
        get_list = requests.get(url=url, headers={'Accept': 'application/json'})

        # Die Kodierung der Antwort auf UTF-8 festlegen
        get_list.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get_list.status_code == 200:

            # Die Antwort als Json auslesen, den Wert aus dem Feld 'total_record_count' auslesen und prüfen, ob dieser
            # 0 ist (= keine Rechnungen am Lieferanten)
            if (get_list.json()['total_record_count'] == 0):

                # Die URL für die API zusammensetzen
                url = 'acq/vendors/{}?apikey={}'.format(alma_api_base_url, vendor_line, api_key)

                # Die GET-Abfrage ausführen
                get = requests.get(url=url, headers={'Accept': 'application/json'})

                # Die Kodierung der Antwort auf UTF-8 festlegen
                get.encoding = 'utf-8'

                # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
                if get.status_code == 200:

                    # Die Antwort als json auslesen, den Wert aus dem Feld 'status/value' auslesen und Prüfen, ob dieser
                    # 'ACTIVE' ist (aktiver Lieferant)
                    if get.json()['status']['value'] == 'ACTIVE':

                        # den Inhalt der Antwort als Text auslesen
                        info = get.text

                        # den Status-Wert "Active" durch "Inactive" ersetzen
                        payload = info.replace('"status":{"value":"ACTIVE","desc":"Active"}',
                                   '"status":{"value":"INACTIVE","desc":"Inactive"}')

                        # Update als PUT-Abfrage ausführen. URL ist die gleiche, Encoding ist wieder utf-8, Inhalt ist JSON
                        update = requests.put(url=url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'})

                        # Die Kodierung der Antwort auf UTF-8 festlegen
                        update.encoding = 'utf-8'

                        # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
                        if update.status_code == 200:
                            logging.info('succesfully updated vendor {}'.format(vendor_line))
                        else:
                            logging.error('problem updating vendor {}:{}'.format(vendor_line, update.text))
                    else:
                        logging.info('account {} is already inactive'.format(vendor_line))
                else:
                    logging.error(get.text)
            else:
                logging.warning('account {} still has {} po-lines'.format(vendor_line, get_list.json()['total_record_count']))
        else:
            logging.error(get_list.text)


def set_liable_for_vat():
    # Datei mit den Lieferanten mit suffix finance laden
    vendors = load_identifier_list_of_type('vendors_finance')

    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for vendor_line in vendors:

        time.sleep(0.05)

        # Die URL für die API zusammensetzen
        url = '{}acq/vendors/{}?apikey={}'.format(alma_api_base_url, vendor_line, api_key)

        # GET-Abfrage ausführen
        get = requests.get(url=url, headers={'Accept': 'application/json'})

        # Kodierung auf UTF-8 festsetzen
        get.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get.status_code == 200:

            # Den Inhalt der Antwort als Text auslesen
            info = get.text

            # In dem Text den Typ auf Inactive ändern
            payload = info.replace('"liable_for_vat":false,', '"liable_for_vat":true,')

            if '"country":{"value":"DE"' in payload:
                payload = payload.replace('"country":{"value":"DE","desc":""}', '"country":{"value":"DEU","desc":"Germany"}')

            if '"country":{"value":"Germany"' in payload:
                payload = payload.replace('"country":{"value":"Germany","desc":""}', '"country":{"value":"DEU","desc":"Germany"}')

            if '"country":{"value":"Deutschland"' in payload:
                payload = payload.replace('"country":{"value":"Deutschland","desc":""}', '"country":{"value":"DEU","desc":"Germany"}')

            if '"country":{"value":"Schweiz"' in payload:
                payload = payload.replace('"country":{"value":"Schweiz","desc":""}', '"country":{"value":"CHE","desc":"Germany"}')

            if '"country":{"value":"Japan"' in payload:
                payload = payload.replace('"country":{"value":"Japan","desc":""}', '"country":{"value":"JPN","desc":"Germany"}')

            if '"country":{"value":"Belgien"' in payload:
                payload = payload.replace('"country":{"value":"Belgien","desc":""}', '"country":{"value":"BEL","desc":"Germany"}')

            # Update als PUT-Abfrage ausführen. URL ist die gleiche, Encoding ist wieder utf-8, Inhalt ist JSON
            update = requests.put(url=url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'})

            # Antwort-Encoding ist wieder UTF-8
            update.encoding = 'utf-8'

            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update.status_code == 200:
                logging.info('succesfully updated liable for vat for vendor {}'.format(vendor_line))
            else:
                logging.warning('problem updating liable for vat for vendor {}:{}'.format(vendor_line, update.text))
        else:
            logging.error(get.text)


def fill_financial_code():
    # Datei mit den Lieferanten mit suffix finance laden
    vendors = load_identifier_list_of_type('vendors_finance')

    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for vendor_line in vendors:

        time.sleep(0.05)

        # Die URL für die API zusammensetzen
        url = '{}acq/vendors/{}?apikey={}'.format(alma_api_base_url, vendor_line, api_key)

        # GET-Abfrage ausführen
        get = requests.get(url=url, headers={'Accept': 'application/json'})

        # Kodierung auf UTF-8 festsetzen
        get.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get.status_code == 200:

            # Den Inhalt der Antwort als JSON-Objekt auslesen
            get_json = get.json()

            try:
                if get_json['financial_sys_code'] != '':
                    logging.info('financial code already set for vendor {}'.format(vendor_line))
                    # den SAP-Kreditoren-Wert aus dem Feld 'additional_code' auslesen
            except KeyError:
                logging.debug('no financial code set for vendor {}'.format(vendor_line))
            try:
                financial_code = get_json['additional_code']
            except KeyError:
                logging.error('no additional code in vendor {}'.format(vendor_line))
                continue
            # den ausgelesenen Wert in das Feld 'financial_sys_code' schreiben
            get_json['financial_sys_code'] = financial_code

            # get_json['additional_code'] = ""

            for index, currency in enumerate(get_json['currency']):
                if currency['value'] == 'DEM':
                    get_json['currency'].pop(index)

            for address in get_json['contact_info']['address']:
                if address['country']['value'] == 'Deutschland':
                     address['country']['value'] = 'DEU'

            # den aktualisierten Lieferanten per PUT-Abfrage nach Alma schreiben (gleiche url wie bei der Abfrage,
            # das JSON-object wird über json.dumps in einen Text umgewandelt.
            update = requests.put(url=url, data=json.dumps(get_json), headers={'Content-Type': 'application/json'})

            # die Kodierung der Antwort setzen
            update.encoding = 'utf-8'

             # Prüfen, ob die Abfrage erfolgreich war (Status Code 200) und alles in die Log-Datei schreiben
            if update.status_code == 200:
                logging.info('succesfully updated vendor {}'.format(vendor_line))
            else:
                logging.error('problem updating vendor {}:{}'.format(vendor_line, update.text))

        else:
            logging.error(get.text)


def update_partners(project):
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for partner in partners:

        # Die URL für die API zusammensetzen
        url = '{}partners/{}?apikey={}'.format(alma_api_base_url, partner, api_key)

        # Die GET-Abfrage ausführen
        response = requests.get(url=url, headers={'Accept': 'application/json'})

        # Die Kodierung der Antwort auf UTF-8 festlegen
        response.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if response.status_code == 200:

            # Die Antwort als Json auslesen, den Wert aus dem Feld 'total_record_count' auslesen und prüfen, ob dieser
            # 0 ist (= keine Rechnungen am Lieferanten)
            partner_json = response.json()

            try:
                iso_details = partner_json['partner_details']['profile_details']['iso_details']
                iso_details['ill_server'] = 'zfl2-test.hbz-nrw.de'
                iso_details['ill_port'] = '33242'
                iso_symbol = iso_details['iso_symbol']
                if 'DE-' not in iso_symbol:
                    iso_details['iso_symbol'] = 'DE-' + iso_symbol
            except KeyError:
                logging.error('no iso details for partner {}'.format(partner))
            partner_details = partner_json['partner_details']
            partner_details['borrowing_supported'] = True
            partner_details['lending_supported'] = True
            partner_details['borrowing_workflow'] = 'DEFAULT_BOR_WF'
            partner_details['lending_workflow'] = 'DEFAULT_LENDING_WF'

            address_block = partner_json['contact_info']['address'][0]

            google_data = get_google_data(address_block)
            if google_data is not None:
                for address_data in google_data['result']['address_components']:
                    if 'country' in address_data['types']:
                        if address_data['short_name'] == 'DE':
                            partner_json['contact_info']['address'][0]['country'] = {"value": "DEU"}
                    elif 'locality' in address_data['types']:
                        partner_json['contact_info']['address'][0]['city'] = address_data['long_name']

                # Update als PUT-Abfrage ausführen. URL ist die gleiche, Encoding ist wieder utf-8, Inhalt ist JSON
            update = requests.put(url=url, data=json.dumps(partner_json).encode('utf-8'),
                                              headers={'Content-Type': 'application/json'})

            # Die Kodierung der Antwort auf UTF-8 festlegen
            update.encoding = 'utf-8'

            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update.status_code == 200:
                logging.info('succesfully updated partner {}'.format(partner))
            else:
                logging.error('problem updating partner {}:{}'.format(partner, update.text))
        else:
            logging.error(response.text)


def extend_partner_names():
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # API-Key aus den Umgebungsvariablen lesen
    # api_key = os.environ['ALMA_SCRIPT_API_KEY']
    api_key = os.environ['ALMA_API_WUP']

    # alle Lieferanten durchgehen
    for partner in partners:
        time.sleep(0.5)

        # Die URL für die API zusammensetzen
        url = '{}partners/{}?apikey={}'.format(alma_api_base_url, partner, api_key)
        try:
            # Die GET-Abfrage ausführen
            response = requests.get(url=url, headers={'Accept': 'application/json'})

            # Die Kodierung der Antwort auf UTF-8 festlegen
            response.encoding = 'utf-8'

            # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
            if response.status_code == 200:

                # Die Antwort als Json auslesen, den Wert aus dem Feld 'total_record_count' auslesen und prüfen, ob dieser
                # 0 ist (= keine Rechnungen am Lieferanten)
                partner_json = response.json()

                try:
                    if partner_json['partner_details']['profile_details']['profile_type'] == 'SLNP':
                        symbol = partner_json['partner_details']['profile_details']['iso_details']['iso_symbol']
                        if 'DE-' in symbol:
                            symbol = symbol.replace('DE-', '')
                        symbol = 'DE-' + symbol.capitalize()
                        partner_json['partner_details']['profile_details']['iso_details']['iso_symbol'] = symbol
                        name = partner_json['partner_details']['name']
                        if not name.startswith('DE-'):
                            partner_json['partner_details']['name'] = '{} ({})'.format(symbol, name)
                        update = requests.put(url=url, data=json.dumps(partner_json).encode('utf-8'),
                                      headers={'Content-Type': 'application/json'})

                        # Die Kodierung der Antwort auf UTF-8 festlegen
                        update.encoding = 'utf-8'

                        # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
                        if update.status_code == 200:
                            logging.info('succesfully updated partner {}'.format(partner))
                        else:
                            logging.error('problem updating partner {}:{}'.format(partner, update.text))
                    else:
                        logging.warning('could not update partner {}: no SLNP'.format(partner))
                        logging.warning(response.text)
                except KeyError:
                    logging.error('no iso details for partner {}'.format(partner))
            else:
                logging.warning(response.text)
        except:
            logging.error('error upon api connection: {}'.format(partner))


def update_partners_resending_due_interval(project):
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for partner in partners:

        # Die URL für die API zusammensetzen
        url = '{}partners/{}?apikey={}'.format(alma_api_base_url, partner, api_key)

        # Die GET-Abfrage ausführen
        response = requests.get(url=url, headers={'Accept': 'application/json'})

        # Die Kodierung der Antwort auf UTF-8 festlegen
        response.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if response.status_code == 200:

            # Die Antwort als Json auslesen, den Wert aus dem Feld 'total_record_count' auslesen und prüfen, ob dieser
            # 0 ist (= keine Rechnungen am Lieferanten)
            partner_json = response.json()

            try:
                iso_details = partner_json['partner_details']['profile_details']['iso_details']
                iso_details['resending_overdue_message_interval'] = 10
            except KeyError:
                logging.error('no iso details for partner {}'.format(partner))
            update = requests.put(url=url, data=json.dumps(partner_json).encode('utf-8'),
                                              headers={'Content-Type': 'application/json'})

            # Die Kodierung der Antwort auf UTF-8 festlegen
            update.encoding = 'utf-8'

            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update.status_code == 200:
                logging.info('succesfully updated partner {}'.format(partner))
            else:
                logging.error('problem updating partner {}:{}'.format(partner, update.text))
        else:
            logging.error(response.text)


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
                requests_url = '{}users/{}/requests?mms_id={}&apikey={}'.format(alma_api_base_url, user_id, mms_id, api_key)
                request_json = {}
                request_json['request_type'] = 'HOLD'
                request_json['holding_id'] =holding_id
                request_json['pickup_location_type'] = 'LIBRARY'
                request_json['pickup_location_library'] = request['Abholort'].strip()
                request_json['booking_start_date'] = '{}-{}-{}'.format(date[:4], date[4:6], date[6:])
                set_request = requests.post(url=requests_url, data=json.dumps(request_json).encode('utf-8'),
                                      headers={'Content-Type': 'application/json'})

                if set_request.status_code == 200:
                    logging.info('succesfully created request for user {} for mms_id, holding_id {}, {}'.format(user_id, mms_id, holding_id))
                else:
                    logging.error('problem creating request for user {} for mms_id, holding_id {}, {}'.format(user_id, mms_id, holding_id))
                    logging.error(set_request.status_code)
                    logging.error(set_request.text)

            else:
                logging.warning(response.text)
        except:
            logging.error('problem creating request for user {} and item - no response from API'.format(user_id, item_id))







# Haupt-Startpunkt eines jeden Python-Programms.
if __name__ == '__main__':

    # den Namen des Laufs angeben. Dieser definiert den name der Log-Datei und den Typ an Liste, die geladen wird.
    project = 'partners_wup'

    # den Namen der Logdatei festlegen
    log_file = 'data/output/{}.log'.format(project)

    # den Logger konfigurieren (Dateinamen, Level)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=log_file, level=logging.INFO)

    # welcher Lauf gestartet werden soll. Zum Aufrufen einer Funktion den entsprechenden Aufruf durch Entfernen des '#'
    # aktivieren

    # update_users()
    # update_vendors('do')
    # deactivate_non_used_vendors(run_name)
    # check_log(run_name)
    # fill_financial_code()
    # set_liable_for_vat()
    # update_partners(run_name)
    # update_partners_resending_due_interval(project)
    extend_partner_names()
    # set_requests()

