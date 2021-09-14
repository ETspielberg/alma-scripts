import json
import logging
import os

import requests

from lxml import etree

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def get_partner(partner_code):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}partners/{}?apikey={}'.format(alma_api_base_url, partner_code, api_key)
    get = requests.get(url=url, headers={'Accept': 'application/json'})
    get.encoding = 'utf-8'
    if get.status_code == 200:
        logging.debug('retrieved partner {}'.format(partner_code))
        return get.json()
    else:
        logging.warning('could not retrieve partner {}\n{}'.format(partner_code, get.text))
        return None


def update_partner(partner):
    partner_code = partner['partner_details']['code']
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}partners/{}?apikey={}'.format(alma_api_base_url, partner_code, api_key)
    payload = json.dumps(partner)
    update = requests.put(url=url, data=payload.encode('utf-8'),
                          headers={'Content-Type': 'application/json'})

    # Antwort-Encoding ist wieder UTF-8
    update.encoding = 'utf-8'

    # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
    if update.status_code == 200:
        logging.info('succesfully updated partner {}'.format(partner_code))
        return True
    else:
        try:
            update = etree.fromstring(update.content)

        except:
            logging.warning('could parse error response for {}'.format(partner_code))
        logging.warning('problem updating partner {}:{}'.format(partner_code, update.text))
        return False


def save_new_partner(partner):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}partners?apikey={}'.format(alma_api_base_url, api_key)
    # Partner als neuen Partner speichern
    new_partner = requests.post(url=url, data=json.dumps(partner).encode('utf-8'),
                                headers={'Content-Type': 'application/json'})
    # nur wenn das Anlegen erfolgreich war, den alten Partner löschen
    if new_partner.status_code < 300:
        logging.info('succesfully created partner: \n {}'.format(new_partner.text))
        logging.warning(new_partner.text)
        return True
    else:
        logging.warning(new_partner.text)
        return False

def delete_partner(partner_code):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}partners/{}?apikey={}'.format(alma_api_base_url, partner_code, api_key)
    requests.delete(url=url)
    logging.info('deleted partner ' + partner_code)
