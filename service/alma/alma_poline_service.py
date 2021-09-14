import json
import logging
import os

import requests

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def get_poline(poline_number):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}acq/po-lines/{}?apikey={}'.format(alma_api_base_url, poline_number, api_key)
    get = requests.get(url=url, headers={'Accept': 'application/json'})
    get.encoding = 'utf-8'
    if get.status_code == 200:
        logging.debug('retrieved po line {}'.format(poline_number))
        return get.json()
    else:
        logging.warning('could not retrieve po line {}\n{}'.format(poline_number, get.text))
        return None


def update_poline(po_line):
    poline_number = po_line['number']
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}acq/po-lines/{}?apikey={}'.format(alma_api_base_url, poline_number, api_key)
    update = requests.put(url=url, data=json.dumps(po_line),
                          headers={'Content-Type': 'application/json'})
    if update.status_code < 300:
        logging.info('added notes to order {}'.format(poline_number))
    else:
        logging.warning('problem updating order {}: {}'.format(poline_number, update.text))