import logging
from urllib import parse

import requests

import xml.etree.ElementTree as etree

institution_code = '49HBZ_UDE'
alma_domain_name = 'hbz-ubdue'

# base ALMA SRU search API url
sru_alma_search_url = 'https://{}.alma.exlibrisgroup.com/view/sru/{}?version=1.2&operation=searchRetrieve&recordSchema=marcxml&maximumRecords={}&startRecord={}&query={}'


def get_number_of_hits(field, search_term, project):
    query = 'alma.{}={}'.format(field, parse.quote('"' + search_term + '"'))
    logging.debug('running query: {}'.format(query))

    # Die Paramter für eine Suche vorbereiten, um die Anzahld er Treffer auszulesen.
    offset = 0
    limit = 1

    # Die URL für die API zusammensetzen
    url = sru_alma_search_url.format(alma_domain_name, institution_code, limit, offset, query)

    logging.debug('querying url {}'.format(url))

    # Die GET-Abfrage ausführen
    get_list = requests.get(url=url)
    get_list.encoding='utf-8'

    # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
    if get_list.status_code == 200:

        # Antwort als XML auslesen
        content = etree.fromstring(get_list.content)

        # Gesamtzahl der gefundenen Einträge abrufen
        try:
            return int(content.find('{http://www.loc.gov/zing/srw/}numberOfRecords').text)
        except AttributeError:
            logging.error('project {}: no SRU response for entry {}'.format(project, search_term))
            return 0
    else:
        logging.error('project {}: error from SRU: {}'.format(project, get_list.text))
        return 0
