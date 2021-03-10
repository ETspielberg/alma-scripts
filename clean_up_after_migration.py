import json
import logging
import os
import time

import requests

# lädt die Funktion load_identifier_list_of_type aus der Datei service/list_reader_service.py
from cleanup.partners import update_partners, update_partners_resending_due_interval, extend_partner_names
from cleanup.users import update_users, set_requests
from cleanup.vendors import set_liable_for_vat, fill_financial_code, deactivate_non_used_vendors, update_vendors
from service.google_service import get_google_data
from service.list_reader_service import load_identifier_list_of_type

# Basis-URL für die Alma API
from service.table_reader_service import read_requests_table, read_sem_apps_table

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def set_sem_apps():
    table = read_sem_apps_table()
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    # alle Lieferanten durchgehen
    for index, sem_app in table.iterrows():
        time.sleep(0.5)
        primary_identifier = sem_app['PrimaryIdentifier'].strip()
        barcode = sem_app['Barcode'].strip()
        # try:
        logging.debug('processing item with barcode {}'.format(barcode))
        url = '{}items?item_barcode={}&apikey={}'.format(alma_api_base_url, barcode, api_key)
        response = requests.get(url=url, headers={'Accept': 'application/json'})
        response.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if response.status_code == 200:
            item = response.json()
            mms_id = item['bib_data']['mms_id']
            holding_id = item['holding_data']['holding_id']
            item_id = item['item_data']['pid']
            library = item['item_data']['library']['value']
            sem_app_user_url = '{}users/{}?apikey={}'.format(alma_api_base_url, primary_identifier, api_key)
            sem_app_response = requests.get(url=sem_app_user_url, headers={'Accept': 'application/json'})
            if sem_app_response.status_code == 200:
                sem_app_response.encoding = 'utf-8'
                sem_app = sem_app_response.json()
                for address in sem_app['contact_info']['address']:
                    print(address)
                    if address['preferred']:
                        print('building item')
                        item['item_data']['public_note'] = address['line1']
                        item['holding_data']['in_temp_location'] = True
                        item['holding_data']['temp_library']['value'] = library
                        if library == 'E0001':
                            item['holding_data']['temp_location']['value'] = 'ESA'
                        elif library == 'D0001':
                            item['holding_data']['temp_location']['value'] = 'DSA'
                        elif library == 'E0023':
                            item['holding_data']['temp_location']['value'] = 'MSA'
                item_url = '{}bibs/{}/holdings/{}/items/{}?apikey={}'.format(alma_api_base_url, mms_id, holding_id,
                                                                             item_id, api_key)
                # print(json.dumps(item))
                update = requests.put(url=item_url, data=json.dumps(item).encode('utf-8'),
                                      headers={'Content-Type': 'application/json'})
                # Die Kodierung der Antwort auf UTF-8 festlegen
                update.encoding = 'utf-8'

                # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
                if update.status_code == 200:
                    logging.info('succesfully updated item {}'.format(barcode))
                else:
                    logging.error('problem updating item {}:{}'.format(barcode, update.text))
        else:
            logging.warning(response.text)
# except:
#    logging.error('problem processing item with barcode {} loaned to sem app {} - no response from API'.format(barcode, primary_identifier))


# Haupt-Startpunkt eines jeden Python-Programms.
if __name__ == '__main__':
    # den Namen des Laufs angeben. Dieser definiert den name der Log-Datei und den Typ an Liste, die geladen wird.
    project = 'partners2'

    # den Namen der Logdatei festlegen
    log_file = 'data/output/{}.log'.format(project)

    # den Logger konfigurieren (Dateinamen, Level)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=log_file, level=logging.DEBUG)

    # welcher Lauf gestartet werden soll. Zum Aufrufen einer Funktion den entsprechenden Aufruf durch Entfernen des '#'
    # aktivieren

    # Benutzerbezogene Updates (siehe cleanup/users)
    # update_users(project)
    # set_requests()

    # Lieferantenbezogene Updates (siehe cleanup/vendors)
    # update_vendors('do')
    # deactivate_non_used_vendors(project)
    # check_log(run_name)
    # fill_financial_code()
    # set_liable_for_vat()

    # Fernleihpartnerbezogene Updates (siehe cleanup/partners)
    update_partners(project)
    # update_partners_resending_due_interval(project)
    # extend_partner_names()

    # sonstige Updates
    # set_sem_apps()

