import copy
import json
import logging
import math
import os
from threading import Thread

import requests

from service import table_reader_service
from service.alma import alma_bib_service, alma_electronic_service
from service.alma.alma_bib_service import retrieve_holding_record
from service.list_reader_service import load_identifier_list_of_type
from service.table_reader_service import read_alma_holdings_list, read_table
from utils import chunks

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'

surpressed_locations = ['ENP', 'DNP', 'MNP']


def add_dummy_item_to_holding(project):
    mms_ids = read_alma_holdings_list(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(mms_ids) / number_of_threads)
    list_chunks = list(chunks(mms_ids, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=add_dummy_item_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def collect_items(mms_id, holding_id):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    limit = 20
    offset = 0
    item_request_url = '{}bibs/{}/holdings/{}/items?apikey={}&limit={}&offset={}'.format(alma_api_base_url, mms_id,
                                                                                         holding_id, api_key, limit,
                                                                                         offset)
    response = requests.get(url=item_request_url, headers={'Accept': 'application/json'})

    # Die Kodierung der Antwort auf UTF-8 festlegen
    response.encoding = 'utf-8'

    # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
    if response.status_code == 200:
        logging.info('got item list for mms id {}'.format(mms_id))
        number_of_items = response.json()['total_record_count']
        if number_of_items <= 0:
            logging.error('no items found')
            return None
        try:
            item_list = response.json()['item']
        except KeyError:
            return None
        while len(item_list) < number_of_items:
            offset += limit
            item_request_url = '{}bibs/{}/holdings/{}/items?apikey={}&limit={}&offset={}'.format(alma_api_base_url,
                                                                                                 mms_id,
                                                                                                 holding_id, api_key,
                                                                                                 limit,
                                                                                                 offset)
            response = requests.get(url=item_request_url, headers={'Accept': 'application/json'})
            response.encoding = 'utf-8'
            try:
                item_list = item_list + response.json()['item']
            except KeyError:
                break
        return item_list
    else:
        logging.warning(
            'could not retrieve items for mms id {} and holding {}: {}'.format(mms_id, holding_id, response.text))
    return None


def check_temporary_location(list_item):
    for item in list_item:
        try:
            temporary_location = item['holding_data']['temp_location']['value']
            if temporary_location not in surpressed_locations:
                mms_id = item['bib_data']['mms_id']
                holding_id = item['holding_data']['holding_id']
                item_id = item['item_data']['pid']
                barcode = item['item_data']['barcode']
                logging.info("item ist not surpressed : |{}|{}|{}|{}".format(mms_id, holding_id, item_id, barcode))
                return False
        except KeyError:
            return False
    return True


def create_dummy_item(item):
    dummy_item = {
        "link": "",
        "holding_data": {
            "link": "",
            "holding_id": item['holding_data']['holding_id'],
            "in_temp_location": False,
            "temp_library": {
                "value": ""
            },
            "temp_location": {
                "value": ""
            },
            "temp_call_number_type": {
                "value": ""
            },
            "temp_call_number": "",
            "temp_policy": {
                "value": ""
            }
        },
        "item_data": {
            "barcode": "",
            "physical_material_type": {
                "value": item['item_data']['physical_material_type']['value']
            },
            "policy": {
                "value": item['item_data']['policy']['value']
            },
            "provenance": {
                "value": ""
            },
            "po_line": "",
            "is_magnetic": False,
            "description": "",
            "alternative_call_number": item['item_data']['alternative_call_number'],
            "alternative_call_number_type": {
                "value": item['item_data']['alternative_call_number_type']['value']
            }
        }
    }
    return dummy_item


def save_item(mms_id, dummy_item):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    logging.info('saving dummy item for MMS ID {}'.format(mms_id))
    # Die URL für die API zusammensetzen
    holding_id = dummy_item['holding_data']['holding_id']
    url = '{}bibs/{}/holdings/{}/items?apikey={}&limit=50&offset=0'.format(alma_api_base_url, mms_id, holding_id,
                                                                           api_key)
    response = requests.post(url=url, data=json.dumps(dummy_item).encode('utf-8'),
                             headers={'Content-Type': 'application/json', 'Accept': 'application/json'})

    # Die Kodierung der Antwort auf UTF-8 festlegen
    response.encoding = 'utf-8'
    if response.status_code < 300:
        logging.info('created item: ' + response.text)
        try:
            item_id = response.json()['item_data']['pid']
            logging.info(
                'successfully created dummy item for MMS ID and holding: |{}|{}|{}'.format(item_id, mms_id, holding_id))
        except KeyError:
            logging.error('could not parse response')
    else:
        logging.error(
            'could not creat dummy item for MMS ID and holding : |{}|{}|{}'.format(mms_id, holding_id, response.text))
    pass


def add_dummy_item_for_chunk(chunk_list, test='test'):
    # alle Lieferanten durchgehen
    for index, row in chunk_list.iterrows():
        mms_id = row['MMS-ID']
        holding_id = row['Bestands-ID']
        logging.info('processing MMS ID {} and holding {}'.format(mms_id, holding_id))
        list_item = collect_items(mms_id, holding_id)
        if list_item is None:
            logging.info("no items found for MMS-ID und Holding |{}|{}".format(mms_id, holding_id))
            continue
        all_surpressed = check_temporary_location(list_item)
        if all_surpressed:
            dummy_item = create_dummy_item(item=list_item[0])
            save_item(mms_id, dummy_item)
        else:
            logging.info('not all items are surpressed MMS ID {} and holding {}'.format(mms_id, holding_id))


def add_125_field(project):
    field_list = read_table(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(field_list) / number_of_threads)
    list_chunks = list(chunks(field_list, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=add_125_field_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def add_125_field_for_chunk(chunk_list, test='test'):
    for index, row in chunk_list.iterrows():
        ht_number = row['HT-Nummer'].strip()
        mms_id = alma_bib_service.retrieve_mms_id(ht_number)
        if mms_id is None:
            logging.warning('found no mms id for ht number {}'.format(ht_number))
            continue
        library = row['OWN_1'].strip()
        number_of_portfolios = alma_electronic_service.get_number_of_portfolios(mms_id)
        if number_of_portfolios > 0:
            logging.info('electronic title: | {} | {}'.format(ht_number, mms_id))
            continue
        holdings = alma_bib_service.retrieve_holdings(mms_id, library)
        if len(holdings) == 0:
            logging.warning('found no holdings for | {} | {}'.format(ht_number, mms_id))
            continue
        fields = []
        for i in range(1, 12):
            field = row['Feld 125_{}'.format(i)]
            if type(field) != str:
                continue
            fields.append(field.strip())
        for holding in holdings:
            holding_record = alma_bib_service.retrieve_holding_record(mms_id, holding['holding_id'])
            updated = alma_bib_service.add_991s_to_holding_record(holding_record, fields)
            if updated:
                alma_bib_service.update_holding_record(mms_id, holding['holding_id'], holding_record, ht_number)


def set_holding_signatures(project):
    holdings_list = read_table(project)
    logging.info('{} holdings in list'.format(len(holdings_list)))
    holdings_list.drop_duplicates(inplace=True)
    logging.info('{} holdings in deduplicated list'.format(len(holdings_list)))
    number_of_threads = 8
    length_of_chunks = math.ceil(len(holdings_list) / number_of_threads)
    list_chunks = list(chunks(holdings_list, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=set_holding_shelfmark_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def set_holding_shelfmark_for_chunk(chunk_list, test='test'):
    for index, row in chunk_list.iterrows():
        mms_id = row['MMS-ID'].strip()
        holding_id = row['Holding-ID'].strip()
        holding_signature = row['Holding-Signatur'].strip()
        holding_record = alma_bib_service.retrieve_holding_record(mms_id, holding_id)
        if holding_record is not None:
            alma_bib_service.add_signature_to_holding_record(mms_id=mms_id, holding_id=holding_id, holding_record=holding_record, signature=holding_signature)
        else:
            logging.warning('no holding could be retrieved | {} | {}'.format(mms_id, holding_id))


def correct_holding_signatures(project):
    holding_list = table_reader_service.read_table(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(holding_list) / number_of_threads)
    list_chunks = list(chunks(holding_list, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=correct_holding_signatures_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def correct_holding_signatures_for_chunk(chunk, test='test'):
    for index, row in chunk.iterrows():
        mms_id = row['MMS Id']
        holding_id = row['Holding Id']
        signature_current = row['Permanent Call Number']
        signature_current = signature_current.replace('_d', '')
        signature = signature_current[:2] + ' ' + signature_current[2] + ' ' + signature_current[3:]
        print(signature)
        holding_record = alma_bib_service.retrieve_holding_record(mms_id, holding_id)
        alma_bib_service.add_signature_to_holding_record(mms_id=mms_id, holding_id=holding_id, holding_record=holding_record, signature=signature)

