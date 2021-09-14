import json
import logging
import math
import os
from threading import Thread

import requests

from service.list_reader_service import load_identifier_list_of_type
from utils import chunks

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def update_portfolio_list(project):
    ht_numbers = load_identifier_list_of_type(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(ht_numbers) / number_of_threads)
    list_chunks = list(chunks(ht_numbers, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=update_portfolio_list_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def delete_portfolios(project):
    portfolio_urls = load_identifier_list_of_type(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(portfolio_urls) / number_of_threads)
    list_chunks = list(chunks(portfolio_urls, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=delete_portfolios_for_chunk,
                        args=(chunk, 'test'))
        thread.start()

def delete_portfolios_for_chunk(chunk_list, test='test'):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    for url in chunk_list:
        delete_url = '{}?apikey={}'.format(url, api_key)
        delete_response = requests.delete(url=delete_url)
        if delete_response.status_code < 300:
            logging.info('deleted portfolio with url {}'.format(url))
        else:
            logging.warning('could not delete portfolio with url'.format(url))


def update_portfolio_list_for_chunk(chunk_list, test='test'):
    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for mms_id in chunk_list:
        logging.info('processing MMS ID {}'.format(mms_id))

        # Die URL für die API zusammensetzen
        url = '{}bibs/{}/portfolios?apikey={}&limit=25'.format(alma_api_base_url, mms_id, api_key)

        # Die GET-Abfrage ausführen
        response = requests.get(url=url, headers={'Accept': 'application/json'})

        # Die Kodierung der Antwort auf UTF-8 festlegen
        response.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if response.status_code == 200:
            logging.info('got portfolio list {} : \n {}'.format(mms_id, response.text))
            # Die Antwort als Json auslesen, den Wert aus dem Feld 'total_record_count' auslesen und prüfen, ob dieser
            # 0 ist (= keine Rechnungen am Lieferanten)
            try:
                portfolio_list = response.json()['portfolio']
            except KeyError:
                logging.warning('no portfolios found for mms id {}'.format(mms_id))
                continue
            number_of_portfolios = response.json()['total_record_count']
            if number_of_portfolios == 0:
                logging.warning('no portfolios found for mms id {}'.format(mms_id))
                continue
            if number_of_portfolios != len(portfolio_list):
                logging.warning(
                    'portfolios list size and number of portfolios does not match for mms id {}'.format(mms_id))
                continue
            portfolio_list_dict = {}
            for portfolio in portfolio_list:
                portfolio_id = portfolio['id']
                if portfolio['availability']['value'] == '10':
                    logging.warning('portfolio with mms {} and id {} is inactive'.format(mms_id, portfolio_id))
                    continue
                portfolio_url = '{}bibs/{}/portfolios/{}?apikey={}'.format(alma_api_base_url, mms_id, portfolio_id,
                                                                           api_key)
                portfolio_response = requests.get(url=portfolio_url, headers={'Accept': 'application/json'})
                portfolio_response.encoding = 'utf-8'
                if portfolio_response.status_code == 200:
                    full_portfolio = portfolio_response.json()
                    linking_url = full_portfolio['linking_details']['static_url']
                    if linking_url == '':
                        linking_url = full_portfolio['linking_details']['parser_parameters_override']
                    if linking_url not in portfolio_list_dict:
                        portfolio_list_dict[linking_url] = []
                        logging.info('created new group for mms id {} and linking url {}'.format(mms_id, linking_url))
                    portfolio_list_dict[linking_url].append(full_portfolio)
            try:
                for key, portfolio_group in portfolio_list_dict.items():
                    clean_portfolio_group(portfolio_group)
            except:
                continue


def clean_portfolio_group(portfolio_group):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    public_notes = []
    authentication_notes = []

    for portfolio in portfolio_group:
        if portfolio['authentication_note'] != '':
            authentication_note = portfolio['authentication_note']
            if authentication_note not in authentication_notes:
                authentication_notes.append(authentication_note)
        if portfolio['public_note'] != '':
            public_note = portfolio['public_note']
            if public_note not in public_notes:
                public_notes.append(public_note)
    public_note = ''
    for note in public_notes:
        if public_note != '':
            public_note += ', '
        public_note += note
    logging.debug('built public note: {}'.format(public_note))
    authentication_note = ''
    for note in authentication_notes:
        if note != '':
            authentication_note += ', '
        authentication_note += note
    logging.debug('built authentication note: {}'.format(authentication_note))
    for index, portfolio in enumerate(portfolio_group):
        mms_id = portfolio['resource_metadata']['mms_id']['value']
        portfolio_id = portfolio['id']
        if index == 0:
            portfolio['authentication_note'] = authentication_note
            portfolio['public_note'] = public_note
            portfolio['library']['value'] = ''
            portfolio['library']['link'] = None
            logging.info('updated portfolio: {}'.format(json.dumps(portfolio)))
            update_url = "{}?apikey={}".format(portfolio['link'], api_key)
            portfolio_response = requests.put(url=update_url, data=json.dumps(portfolio),
                                              headers={'Content-Type': 'application/json'})
            if portfolio_response.status_code == 200:
                logging.info('updated portfolio {} with MMS ID {}'.format(portfolio_id, mms_id))
            else:
                logging.warning('could not update portfolio {} with MMS ID {}: {}'.format(portfolio_id, mms_id,
                                                                                          portfolio_response.text))
        else:
            logging.info('portfolio to delete: {}'.format(portfolio['link']))
