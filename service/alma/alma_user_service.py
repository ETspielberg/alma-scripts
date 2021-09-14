import json
import logging
import os

import requests

from lxml import etree

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def get_user(user_id):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}users/{}?apikey={}'.format(alma_api_base_url, user_id, api_key)
    get = requests.get(url=url, headers={'Accept': 'application/json'})
    get.encoding = 'utf-8'
    if get.status_code == 200:
        logging.debug('retrieved user {}'.format(user_id))
        return get.json()
    else:
        logging.warning('could not retrieve user {}\n{}'.format(user_id, get.text))
        return None


def update_user(user):
    user_id = user['primary_id']
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}users/{}?apikey={}'.format(alma_api_base_url, user_id, api_key)
    payload = json.dumps(user)
    update = requests.put(url=url, data=payload.encode('utf-8'),
                          headers={'Content-Type': 'application/json'})

    # Antwort-Encoding ist wieder UTF-8
    update.encoding = 'utf-8'

    # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
    if update.status_code == 200:
        logging.info('succesfully updated user {}'.format(user_id))
        return True
    else:
        try:
            update = etree.fromstring(update.content)

        except:
            logging.warning('could parse error response for {}'.format(user_id))
        logging.warning('problem updating user {}:{}'.format(user_id, update.text))
        return False
