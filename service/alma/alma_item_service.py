import json
import logging
import os
import urllib

import requests
from lxml import etree

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def get_item(mms_id, item_id, holding_id='ALL'):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}bibs/{}/holdings/{}/items/{}?apikey={}'.format(alma_api_base_url, mms_id, holding_id, item_id, api_key)
    get = requests.get(url=url, headers={'Accept': 'application/json'})
    get.encoding = 'utf-8'
    if get.status_code == 200:
        logging.debug('retrieved item with MMS ID {} and item ID {}'.format(mms_id, item_id))
        return get.json()
    else:
        logging.warning('could not retrieve item with MMS ID {} and item ID {}\n{}'.format(mms_id, item_id, get.text))
        return None


def get_number_of_items(mms_id, holding_id='ALL'):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # Die URL für die API zusammensetzen
    url = '{}bibs/{}/holdings/{}/items?apikey={}'.format(alma_api_base_url, mms_id, holding_id, api_key)

    # Die GET-Abfrage ausführen
    response = requests.get(url=url, headers={'Accept': 'application/json'})

    # Die Kodierung der Antwort auf UTF-8 festlegen
    response.encoding = 'utf-8'

    # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
    if response.status_code == 200:
        number_of_items = response.json()['total_record_count']
        logging.debug('found {} items for mms ID {} and holding ID {}'.format(number_of_items, mms_id, holding_id))
        return number_of_items
    else:
        logging.warning("could not retrieve number of item for mms ID {} and holding ID {}:\n {} ".format(mms_id, holding_id,response.text))
        return None


def get_item_by_barcode(barcode):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}items?item_barcode={}&apikey={}'.format(alma_api_base_url, urllib.parse.quote_plus(barcode), api_key)
    get = requests.get(url=url, headers={'Accept': 'application/json'})
    get.encoding = 'utf-8'
    if get.status_code == 200:
        logging.debug('retrieved item with barcode {}'.format(barcode))
        return get.json()
    else:
        logging.warning('could not retrieve item with barcode {} \n{}'.format(barcode, get.text))
        return None


def update_item(item):
    mms_id = item['bib_data']['mms_id']
    holding_id = item['holding_data']['holding_id']
    item_id = item['item_data']['pid']
    barcode = item['item_data']['barcode']
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}bibs/{}/holdings/{}/items/{}?apikey={}'.format(alma_api_base_url, mms_id, holding_id, item_id, api_key)
    payload = json.dumps(item)
    update = requests.put(url=url, data=payload.encode('utf-8'),
                          headers={'Content-Type': 'application/json'})

    # Antwort-Encoding ist wieder UTF-8
    update.encoding = 'utf-8'

    # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
    if update.status_code == 200:
        logging.info('succesfully updated item (MMS ID, holding ID, item ID, barcode): | {}|{}|{}|{}'.format(mms_id,holding_id,item_id, barcode))
        return True, item
    else:
        try:
            update = etree.fromstring(update.content)
        except:
            logging.warning('could parse error response for item (MMS ID, holding ID, item ID, barcode): | {}|{}|{}|{}'.format(mms_id,holding_id,item_id, barcode))
        logging.warning('problem updating  item (MMS ID, holding ID, item ID, barcode): | {}|{}|{}|{}\n {}'.format(mms_id,holding_id,item_id, barcode, update.text))
        return False, item


def scan_into_department(item, department):
    mms_id = item['bib_data']['mms_id']
    holding_id = item['holding_data']['holding_id']
    item_id = item['item_data']['pid']
    barcode = item['item_data']['barcode']
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    if department == 'Umarbeitung':
        url = '{}bibs/{}/holdings/{}/items/{}?op=scan&department=Umarbeitung&apikey={}'.format(alma_api_base_url, mms_id, holding_id, item_id, api_key)
    elif department == 'Buchbinder':
        url = '{}bibs/{}/holdings/{}/items/{}?op=scan&department=Buchbinder&apikey={}'.format(alma_api_base_url, mms_id, holding_id, item_id, api_key)
    elif department == 'AcqDeptE0001':
        url = '{}bibs/{}/holdings/{}/items/{}?op=scan&department=AcqDeptE0001&library=E0001&apikey={}'.format(alma_api_base_url, mms_id, holding_id, item_id, api_key)
    elif department == 'AcqDeptD0001':
        url = '{}bibs/{}/holdings/{}/items/{}?op=scan&department=AcqDeptD0001&library=D0001&apikey={}'.format(alma_api_base_url, mms_id, holding_id, item_id, api_key)
    else:
        url = '{}bibs/{}/holdings/{}/items/{}?op=scan&department=AcqDeptE0023&library=E0023&apikey={}'.format(
            alma_api_base_url, mms_id, holding_id, item_id, api_key)
    logging.info('performing scan with url: ' + url)
    scan_in = requests.post(url, {}, headers={'Accept': 'application/json'})
    if scan_in.status_code < 300:
        updated = scan_in.json()
        if updated['item_data']['work_order_at']['value'] == department:
            logging.info('scanned in item with (MMS ID, item ID, barcode) |{}|{}|{}'.format(mms_id, item_id, barcode))
            return True
        else:
            logging.warning(
                'scan in operation did not yield desired result for (MMS ID, item ID, barcode) |{}|{}|{}\n{}'.format(mms_id, item_id, barcode, json.dumps(updated)))
            return False
    else:
        logging.warning('could not scan in item with (MMS ID, item ID, barcode) |{}|{}|{}\n{}'.format(mms_id, item_id, barcode, scan_in.text))
        return False


def scan_out_of_department(item, done='false'):
    mms_id = item['bib_data']['mms_id']
    holding_id = item['holding_data']['holding_id']
    item_id = item['item_data']['pid']
    barcode = item['item_data']['barcode']
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    if 'work_order_at' in item['item_data']:
        department = item['item_data']['work_order_at']['value']
    else:
        logging.warning('no work order department given for item')
        logging.warning(item)
        return item
    if department == 'Umarbeitung':
        url = '{}bibs/{}/holdings/{}/items/{}?op=scan&department={}&apikey={}&done={}'.format(alma_api_base_url, mms_id, holding_id, item_id, department, api_key, done)
    elif department == 'Buchbinder':
        url = '{}bibs/{}/holdings/{}/items/{}?op=scan&department={}&apikey={}&done={}'.format(alma_api_base_url, mms_id, holding_id, item_id, department, api_key, done)
    elif department == 'AcqDeptE0001':
        url = '{}bibs/{}/holdings/{}/items/{}?op=scan&department={}&library=E0001&apikey={}&done={}'.format(alma_api_base_url, mms_id, holding_id, item_id, department, api_key, done)
    elif department == 'AcqDeptD0001':
        url = '{}bibs/{}/holdings/{}/items/{}?op=scan&department={}&library=D0001&apikey={}&done={}'.format(alma_api_base_url, mms_id, holding_id, item_id, department, api_key, done)
    else:
        url = '{}bibs/{}/holdings/{}/items/{}?op=scan&department={}&library=E0023&apikey={}&done={}'.format(alma_api_base_url, mms_id, holding_id, item_id, department, api_key, done)
    scan_out = requests.post(url, {}, headers={'Accept': 'application/json'})
    logging.info(scan_out)
    logging.info(url)
    if scan_out.status_code < 300:
        updated = scan_out.json()
        if 'work_order_at' in updated['item_data']:
            logging.warning(
                'scan out operation did not yield desired result for (MMS ID, item ID, barcode) |{}|{}|{}\n{}'.format(mms_id,item_id,barcode,json.dumps(updated)))
            return updated
        else:
            logging.debug('scanned out item with (MMS ID, item ID, barcode) |{}|{}|{}'.format(mms_id, item_id, barcode))
            return updated
    else:
        logging.warning('could not scan out item with (MMS ID, item ID, barcode) |{}|{}|{}\n{}'.format(mms_id, item_id, barcode, scan_out.text))
        return item


def scan_in_at_location(item, location):
    mms_id = item['bib_data']['mms_id']
    holding_id = item['holding_data']['holding_id']
    item_id = item['item_data']['pid']
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    scan_in_url = '{}bibs/{}/holdings/{}/items/{}?apikey={}&op=scan&library={}&circ_desk=DEFAULT_CIRC_DESK'.format(
        alma_api_base_url, mms_id, holding_id, item_id,
        api_key, location)
    scan_in = requests.post(scan_in_url, {})
    if scan_in.status_code < 300:
        logging.info(
            'succesfully scanned in item {} with mms-id {} and holding id {}'.format(item_id, mms_id,
                                                                                     holding_id))