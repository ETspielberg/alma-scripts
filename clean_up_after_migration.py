import json
import logging
import os
import time

import requests

# lädt die Funktion load_identifier_list_of_type aus der Datei service/list_reader_service.py
from service.list_reader_service import load_identifier_list_of_type

# Basis-URL für die Alma API
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


# Haupt-Startpunkt eines jeden Python-Programms.
if __name__ == '__main__':

    # den Namen des Laufs angeben. Dieser definiert den name der Log-Datei und den Typ an Liste, die geladen wird.
    run_name = 'vendors_finance'

    # den Namen der Logdatei festlegen
    log_file = 'data/output/{}.log'.format(run_name)

    # den Logger konfigurieren (Dateinamen, Level)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=log_file, level=logging.INFO)

    # welcher Lauf gestartet werden soll. Zum Aufrufen einer Funktion den entsprechenden Aufruf durch Entfernen des '#'
    # aktivieren

    # update_users()
    # update_vendors('do')
    # deactivate_non_used_vendors(run_name)
    # check_log(run_name)
    # fill_financial_code()
    set_liable_for_vat()

