import logging
import math
import os
from itertools import islice

import requests


alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'

def list_to_string(id_list):
    '''
    writes a list of identifiers as semicolon separated list into a single string
    :param id_list: a list of identifiers
    :return: a string, with the identifiers separated by ';'
    '''
    if len(id_list) == 0:
        return ''
    else:
        string = id_list.pop()
        while len(id_list) > 0:
            string = string + ';' + id_list.pop()
        return string



def get_mms_id(ht_number):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    ht_url = '{}bibs?other_system_id={}&apikey={}'.format(alma_api_base_url, ht_number, api_key)
    response = requests.get(url=ht_url, headers={'Accept': 'application/json'})

    # Die Kodierung der Antwort auf UTF-8 festlegen
    response.encoding = 'utf-8'

    # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
    if response.status_code == 200:
        list_bibs = response['bib']
        if len(list_bibs) != 1:
            logging.warning('found multiple mms ids for ht number {}'.format(ht_number))
            return None
        else:
            return list_bibs[0]['mms_id']


# cuts lists into chunks
# Thanks to Ned Batchelder on Stack overflow (https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks)
def chunks(l, n):
    """Yield successive n-sized chunks from list l
    :parameter l inital lists
    :parameter n number of chunks

    returns an array of arrays
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def chunk_dict(data, number):
    SIZE = math.ceil(len(data)/number)
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k: data[k] for k in islice(it, SIZE)}
