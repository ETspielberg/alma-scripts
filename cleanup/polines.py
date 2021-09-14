import json
import logging
import math
import os
from threading import Thread

import requests

from service import table_reader_service
from service.alma import alma_poline_service
from service.alma.alma_poline_service import update_poline, get_poline
from service.list_reader_service import load_identifier_list_of_type
from service.table_reader_service import read_poline_notes_list
from utils import chunks, chunk_dict

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def add_note_to_poline(project):
    mms_ids = read_poline_notes_list(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(mms_ids) / number_of_threads)
    list_chunks = list(chunks(mms_ids, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=add_note_to_poline_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def add_note_to_poline_for_chunk(chunk_list, test='test'):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    for index, row in chunk_list.iterrows():
        poline = row['poline']
        note = row['note']
        url = '{}acq/po-lines/{}?apikey={}'.format(alma_api_base_url, poline, api_key)
        get = requests.get(url=url, headers={'Accept': 'application/json'})
        get.encoding = 'utf-8'
        logging.debug(get.status_code)
        if get.status_code == 200:
            polines_json = get.json()
            notes = polines_json['note']
            if notes is None:
                notes = []
            notes.append({"note_text": note})
            update = requests.put(url=url, data=json.dumps(polines_json).encode('utf-8'),
                                  headers={'Content-Type': 'application/json'})

            # Antwort-Encoding ist wieder UTF-8
            update.encoding = 'utf-8'

            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update.status_code == 200:
                logging.info('succesfully updated poline {}'.format(poline))
            else:
                logging.warning('problem updating poline {}:{}'.format(poline, update.text))
        else:
            logging.error(get.text)


def prepare_note_lists(project):
    # Die csv-Datei einlesen
    table = table_reader_service.read_arrival_information_csv(project)

    # die Bestellnummer und die Bestellung initialisieren
    note_lists = {}
    # Alle Zeilen durchgehen
    for index, row in table.iterrows():
        # Wenn es sich um einen weiteren Eintrag zur aktuellen Bestellung handelt, den Notiztext den Notizen hinzufügen
        note_text = '{} ({})'.format(row['arrival_note'], row['arrival_date'])
        order_number = row['order_number'].strip()

        if order_number not in note_lists:
            note_lists[order_number] = []
        note_lists[order_number].append({"note_text": note_text})
    return note_lists


def update_order_for_dict_chunk(notes_dict, test='test'):
    for order_number, notes_list in notes_dict.items():
        json_order = get_poline(order_number)
        if json_order is not None:
            # skip all non standing orders
            if 'SO' not in json_order['type']['value']:
                logging.warning('order {} is not a standing order'.format(order_number))
                continue
            if 'note' not in json_order:
                json_order['note'] = notes_list
            elif json_order['note'] == '':
                json_order['note'] = notes_list
            else:
                json_order['note'] = json_order['note'] + notes_list
            update_poline(json_order)
    logging.info('finished')


# Haupt-Startpunkt eines jeden Python-Programms.
def update_order_for_dict(note_lists):
    for chunk in chunk_dict(note_lists, 8):
        thread = Thread(target=update_order_for_dict_chunk,
                        args=(chunk, 'test'))
        thread.start()


def update_notes(project):
    notes = prepare_note_lists(project)
    update_order_for_dict(notes)


def transfer_konsol_note(project):
    po_references = load_identifier_list_of_type(project)
    number_of_threads = 10
    length_of_chunks = math.ceil(len(po_references) / number_of_threads)
    list_chunks = list(chunks(po_references, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=transfer_konsol_note_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def transfer_konsol_note_for_chunk(chunk, test='test'):
    for po_reference in chunk:
        logging.info('processing po line {}'.format(po_reference))
        poline = alma_poline_service.get_poline(po_reference)
        updated = False
        if 'note' not in poline:
            logging.info('no notes in po line {}'.format(po_reference))
            continue
        for note in poline['note']:
            note_text = note['note_text']
            if note_text.lower().strip().startswith('konsol'):
                updated = True
                if 'receiving_note' in poline:
                    receiving_note = poline['receiving_note']
                    if receiving_note.strip == '':
                        poline['receiving_note'] = note_text
                    else:
                        poline['receiving_note'] = 'Konsol. ; \n' + receiving_note
                else:
                    poline['receiving_note'] = note_text
        if updated:
            alma_poline_service.update_poline(poline)
            logging.info('konsol note might be deleted for po line {}'.format(po_reference))

