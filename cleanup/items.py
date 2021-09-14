import logging
import math
import time
from threading import Thread

from service import list_reader_service, table_reader_service
from service.alma import alma_item_service, alma_bib_service
from utils import chunks


def correct_bubi_returns(project, target=None):
    item_list = table_reader_service.read_alma_items_export(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(item_list) / number_of_threads)
    list_chunks = list(chunks(item_list, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=correct_bubi_returns_for_chunk,
                        args=(chunk, target))
        thread.start()


def correct_bubi_returns_for_chunk(chunk, target=None):
    for index, row in chunk.iterrows():
        mms_id = row['MMS ID']
        holding_id = row['Holdings ID']
        item_id = row['Item ID']
        item = alma_item_service.get_item(mms_id=mms_id, item_id=item_id, holding_id=holding_id)
        logging.info(item)
        if target is None:
            target = item['item_data']['location']['value'][0]

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if item is not None:
            # prüfen ob ein barcode existiert. Ansonsten einen Barcode setzen
            if 'barcode' in item['item_data']:
                if item['item_data']['barcode'] == '':
                    item['item_data']['barcode'] = 'ZSN-BD-' + item_id
                    success, item = alma_item_service.update_item(item)
                    time.sleep(1)
            else:
                item['item_data']['barcode'] = 'ZSN-BD-' + item_id
                success, item = alma_item_service.update_item(item)
                time.sleep(1)

            # Exemplar zurückbuchen, wenn és in einer Bearbeitungsabteilung ist.
            if 'work_order_at' in item['item_data']:
                item = alma_item_service.scan_out_of_department(item, 'true')
                time.sleep(1)
            # Temporären Standort und Beschreibung setzen und speichern
            item['holding_data']['temp_location'] = {'value': '{}NP'.format(target)}
            item['holding_data']['in_temp_location'] = True
            item['item_data']['public_note'] = ''
            success, item = alma_item_service.update_item(item)
            if success:
                time.sleep(1)
                if 'work_order_at' in item['item_data']:
                    logging.warning(
                        'item {} still in work order department with mms-id {} and holding id {}'.format(item_id, mms_id, holding_id))
                    continue
                else:
                    alma_item_service.scan_in_at_location(item, target + '0001')
            else:
                logging.warning('problem updating item {} with mms-id {} and holding id {}'.format(item_id, mms_id, holding_id))


def correct_process_type_by_mms_for_chunk(chunk, department):
    for index, row in chunk.iterrows():
        mms_id = row['MMS-ID']
        item_id = row['Exemplar-ID']
        item = alma_item_service.get_item(mms_id, item_id)
        correct_process_type_for_item(item, department)


def correct_process_type_by_mms(project, department):
    item_list = table_reader_service.read_table(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(item_list) / number_of_threads)
    list_chunks = list(chunks(item_list, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=correct_process_type_by_mms_for_chunk,
                        args=(chunk, department))
        thread.start()


def correct_process_type(project, department):
    item_list = list_reader_service.load_identifier_list_of_type(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(item_list) / number_of_threads)
    list_chunks = list(chunks(item_list, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=correct_process_type_for_chunk,
                        args=(chunk, department))
        thread.start()


def correct_process_type_for_item(item, department):
    if item is None:
        logging.warning('item is None')
        return
    try:
        barcode = item['item_data']['barcode']
        if barcode is None:
            barcode = item['item_data']['pid']
        if 'process_type' in item['item_data']:
            if item['item_data']['process_type']['value'] == 'TECHNICAL':
                alma_item_service.scan_into_department(item=item, department=department)
            else:
                logging.warning('item with barcode {} not in Technical - Migration'.format(barcode))
        else:
            logging.warning('item with barcode {} not in process type'.format(barcode))
    except:
        mms_id = item['bib_data']['mms_id']
        holding_id = item['holding_data']['holding_id']
        item_id = item['item_data']['pid']
        logging.warning('item has no barcode: |{}|{}|{}'.format(mms_id, holding_id, item_id))


def set_temporary_location(item, temporary_location, at_temporary_location):
    item['item_data']['temp_location']['value'] = temporary_location
    item['item_data']['temp_location']['desc'] = None
    item['item_data']['in_temp_location'] = at_temporary_location


def add_public_note(item, public_note):
    if item['item_data']['public_note'] is not None:
        if item['item_data']['public_note'] != '':
            item['item_data']['public_note'] = item['item_data']['public_note'] + ', ' + public_note
        else:
            item['item_data']['public_note'] = public_note
    else:
        item['item_data']['public_note'] = public_note


def correct_process_type_for_chunk(chunk, department):
    for barcode in chunk:
        item = alma_item_service.get_item_by_barcode(barcode.strip())
        if item is not None:
            correct_process_type_for_item(item, department)
        else:
            logging.warning('item with barcode {} not in found'.format(barcode))


def correct_locker_data(project):
    locker_list = table_reader_service.read_table(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(locker_list) / number_of_threads)
    list_chunks = list(chunks(locker_list, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=correct_locker_data_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def correct_locker_data_for_chunk(chunk, test='test'):
    for index, row in chunk.iterrows():
        item_id = row['Item Id']
        shelfmark = row['Exemplarsignatur (neu)']
        fulfillment_note = row['Fulfillment Note  (löschen)']
        item = alma_item_service.get_item('ALL', item_id)
        if item is None:
            logging.warning('item is None')
            return
        else:
            old_fulfillment_note = item['item_data']['fulfillment_note']
            if old_fulfillment_note == fulfillment_note:
                item['item_data']['fulfillment_note'] = ''
            item['item_data']['alternative_call_number'] = shelfmark
            item['item_data']['alternative_call_number_type'] = {
                "value": "8",
                "desc": "Other scheme"
            }
            alma_item_service.update_item(item)


def correct_item_shelfmarks(project):
    item_list = table_reader_service.read_table(project)
    number_of_threads = 8
    length_of_chunks = math.ceil(len(item_list) / number_of_threads)
    list_chunks = list(chunks(item_list, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=correct_item_shelfmarks_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def correct_item_shelfmarks_for_chunk(chunk, test='test'):
    for index, row in chunk.iterrows():
        item_id = row['Item Id']
        current_shelfmark = row['aktuelle Exemplarsignatur']
        correct_shelfmark = row['korrekte Exemplarsignatur']
        holding_signature = row['Holdingsignatur']
        item = alma_item_service.get_item('ALL', item_id)
        if item is None:
            logging.warning('item {} is None'.format(item_id))
            continue
        else:
            mms_id = item['bib_data']['mms_id']
            holding_id = item['holding_data']['holding_id']
            old_shelfmark = item['item_data']['alternative_call_number']
            if old_shelfmark == current_shelfmark:
                item['item_data']['alternative_call_number'] = correct_shelfmark
                mms_id = item['bib_data']['mms_id']
                holding_id = item['holding_data']['holding_id']
                logging.info('holding signature: | {} | {} | {} |{}'.format(mms_id, holding_id, item_id, holding_signature))
                alma_item_service.update_item(item)
            elif old_shelfmark == correct_shelfmark:
                logging.info(
                    'signature already corrected: | {} | {} | {} |{}'.format(mms_id, holding_id, item_id, holding_signature))
            else:
                logging.warning('shelfmarks do not match: {} != {} | {} | {} | {} |{}'.format(old_shelfmark, current_shelfmark,mms_id, holding_id, item_id, holding_signature))


def run_function_on_chunks(project, function, additional_argument='test', number_of_threads=8):
    complete_list = table_reader_service.read_table(project)
    length_of_chunks = math.ceil(len(complete_list) / number_of_threads)
    list_chunks = list(chunks(complete_list, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=function,
                        args=(chunk, additional_argument))
        thread.start()


