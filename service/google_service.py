import json
import os
import time

import requests

google_maps_api_base_url = 'https://maps.googleapis.com/maps/api'


def get_google_data(address_lines):
    api_key = os.environ['GOOGLE_MAPS_API_KEY']
    query = ''
    for i in range(1, 5):
        if address_lines['line'+ str(i)] is not None:
            query = query + ' ' + address_lines['line' + str(i)]
    if address_lines['city'] is not None:
        query = query + ' ' + address_lines['city']
    if address_lines['country'] is not None:
        query = query + ' ' + address_lines['country']['value']
    url = '{}/place/findplacefromtext/json?key={}&inputtype=textquery&input={}'.format(google_maps_api_base_url, api_key, query)
    response = requests.get(url=url, headers={'Accept': 'application/json'})

    # Die Kodierung der Antwort auf UTF-8 festlegen
    response.encoding = 'utf-8'

    # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
    if response.status_code == 200:
        time.sleep(1)
        try:
            candidate_id = response.json()['candidates'][0]['place_id']
            url = '{}/place/details/json?place_id={}&fields=address_components&key={}'.format(google_maps_api_base_url, candidate_id, api_key)
            response = requests.get(url=url, headers={'Accept': 'application/json'})
            response.encoding = 'utf-8'
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except IndexError:
            return None
    else:
        return None

