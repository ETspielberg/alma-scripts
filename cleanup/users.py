import hashlib
import json
import logging
import math
import os
import random
import re
import string
from threading import Thread

import requests

import utils
from service import list_reader_service
from service.alma import set_reader_service, alma_user_service
from service.alma.alma_user_service import get_user, update_user
from service.list_reader_service import load_identifier_list_of_type
from service.table_reader_service import read_requests_table, read_password_list
from utils import chunks

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'

postalcode_regexp = pattern = re.compile('(?!01000|99999)(0[1-9]\d{3}|[1-9]\d{4})')


def make_staff__users(project):
    # Datei mit den Benutzerkennungen laden
    users = load_identifier_list_of_type(project)

    # alle Nutzer durchgehen
    for user_id in users:
        user = get_user(user_id)
        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if user is not None:
            # Prüfen, ob der Nutzer Public ist
            if user['record_type']['value'] == 'PUBLIC':

                user['record_type']['value'] = 'STAFF'
                user['record_type']['desc'] = 'Staff'
                update_user(user)
            else:
                logging.info('user {} not public'.format(user))


def set_requests():
    requests_table = read_requests_table()
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for index, request in requests_table.iterrows():
        barcode = request['Barcode'].strip()
        user_id = request['Benutzer_id'].strip()
        try:
            logging.debug('processing item with barcode {}'.format(barcode))
            url = '{}items?item_barcode={}&apikey={}'.format(alma_api_base_url, barcode, api_key)
            response = requests.get(url=url, headers={'Accept': 'application/json'})

            date = request['angelegt am'].strip()
            # Die Kodierung der Antwort auf UTF-8 festlegen
            response.encoding = 'utf-8'

            # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
            if response.status_code == 200:
                response_json = response.json()
                mms_id = response_json['bib_data']['mms_id']
                holding_id = response_json['holding_data']['holding_id']
                requests_url = '{}users/{}/requests?mms_id={}&apikey={}'.format(alma_api_base_url, user_id, mms_id,
                                                                                api_key)
                request_json = {'request_type': 'HOLD', 'holding_id': holding_id,
                                'pickup_location_type': 'LIBRARY',
                                'pickup_location_library': request['Abholort'].strip(),
                                'booking_start_date': '{}-{}-{}'.format(date[:4], date[4:6], date[6:])
                                }
                set_request = requests.post(url=requests_url, data=json.dumps(request_json).encode('utf-8'),
                                            headers={'Content-Type': 'application/json'})

                if set_request.status_code == 200:
                    logging.info(
                        'succesfully created request for user {} for mms_id, holding_id {}, {}'.format(user_id, mms_id,
                                                                                                       holding_id))
                else:
                    logging.error(
                        'problem creating request for user {} for mms_id, holding_id {}, {}'.format(user_id, mms_id,
                                                                                                    holding_id))
                    logging.error(set_request.status_code)
                    logging.error(set_request.text)

            else:
                logging.warning(response.text)
        except:
            logging.error(
                'problem creating request for user {} and item with barcode {} - no response from API'.format(user_id,
                                                                                                              barcode))


def set_pin_pw_and_addresses(project, set_passphrase=False):
    users = load_identifier_list_of_type(project)
    number_of_threads = 10
    length_of_chunks = math.ceil(len(users) / number_of_threads)
    list_chunks = list(chunks(users, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=set_pin_pw_and_addresses_for_chunk,
                        args=(list_chunks[chunk_index], set_passphrase))
        thread.start()


def set_email_addresses(project, email):
    users = load_identifier_list_of_type(project)
    number_of_threads = 10
    length_of_chunks = math.ceil(len(users) / number_of_threads)
    list_chunks = list(chunks(users, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=set_eamil_addresses_for_chunk,
                        args=(list_chunks[chunk_index], email))
        thread.start()


def clean_title(user_json):
    title = user_json['user_title']['value'].lower()
    id = user_json['primary_id']
    if 'prof. dr.' in title:
        user_json['user_title']['value'] = 'PROFDR'
        logging.info("corrected title {} to PROFDR for user {}".format(title, id))
    elif 'prof.' in title:
        user_json['user_title']['value'] = 'PROF'
        logging.info("corrected title {} to PROF for user {}".format(title, id))
    elif 'dr.' in title:
        user_json['user_title']['value'] = 'DR'
        logging.info("corrected title {} to DR for user {}".format(title, id))
    else:
        logging.warning("could not correct title {} for user {}".format(title, id))
    return user_json


def set_eamil_addresses_for_chunk(chunk, email):
    for user_id in chunk:
        user_id = user_id.strip()
        if len(user_id) < 9 and user_id[0].isdigit():
            user_id = user_id.zfill(9)
        if user_id == '':
            continue
        user = get_user(user_id)
        if user is not None:
            user_email = {
                "email_address": email,
                "description": '',
                "preferred": True,
                "segment_type": "Internal",
                "email_type": [
                    {
                        "value": "alternative",
                        "desc": "Alternative"
                    }
                ]
            }
            try:
                user['contact_info']['email'].append(user_email)
            except KeyError:
                user['contact_info']['email'] = []
                user['contact_info']['email'].append(user_email)
            user = clean_title(user)
            update_user(user)


def set_pin_for_chunk(chunk):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    for user in chunk:
        user = user.strip()
        if user == '':
            continue
        url = '{}users/{}?apikey={}'.format(alma_api_base_url, user, api_key)
        get = requests.get(url=url, headers={'Accept': 'application/json'})
        get.encoding = 'utf-8'
        if get.status_code == 200:
            user_json = get.json()
            pin_new = False
            try:
                birth_date_parts = user_json['birth_date'].split('-')
                pin_code = birth_date_parts[2].replace('Z', '') + birth_date_parts[1] + birth_date_parts[0]
                user_json['pin_number'] = pin_code
                pin_new = True
            except:
                logging.warning('could not load birthday for user {}'.format(user))
            if (pin_new):
                payload = json.dumps(user_json)
                update = requests.put(url=url, data=payload.encode('utf-8'),
                                      headers={'Content-Type': 'application/json'})

                # Antwort-Encoding ist wieder UTF-8
                update.encoding = 'utf-8'

                # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
                if update.status_code == 200:
                    logging.info(
                        'succesfully updated user {}'.format(user))
                else:
                    logging.warning('problem updating user {}:{}'.format(user, update.text))
            else:
                logging.error('could not reset pin for user {}'.format(user))


def check_pin(project, update=False):
    users = load_identifier_list_of_type(project)
    number_of_threads = 10
    length_of_chunks = math.ceil(len(users) / number_of_threads)
    list_chunks = list(chunks(users, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=check_pin_for_chunk,
                        args=(chunk, update))
        thread.start()


def check_pin_for_chunk(chunk, update=False):
    for user_id in chunk:
        user = get_user(user_id)
        if user is not None:
            if 'pin_number' in user and user['pin_number'] != '':
                logging.info('pin number already set for user {} '.format(user_id))
                continue
            if 'birth_date' not in user:
                logging.warning('no birth date given for user {} '.format(user_id))
                continue
            else:
                birthday = user['birth_date']
            try:
                pin_number = calculate_standard_pin(birthday)
                if update:
                    user['pin_number'] = pin_number
                    user = clean_title(user)
                    update_user(user)
                else:
                    logging.info('user needs pin update | {} | {}'.format(user_id, pin_number))
            except:
                logging.warning('could not calculate standard pin for user {}'.format(user_id))


def calculate_standard_pin(birthday):
    birthday = birthday.replace('Z', '')
    birth_date_parts = birthday.split('-')
    pin_code = birth_date_parts[2] + birth_date_parts[1] + birth_date_parts[0]
    return pin_code


def clean_addresses(project):
    users = load_identifier_list_of_type(project)
    number_of_threads = 2
    length_of_chunks = math.ceil(len(users) / number_of_threads)
    list_chunks = list(chunks(users, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=clean_first_lines_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def clean_first_lines_for_chunk(chunk, test):
    for user in chunk:
        user = user.strip()
        if user == '':
            continue
        user_json = get_user(user)
        if user_json is not None:
            if user_json['user_group']['value'] == '10':
                continue
            if user_json['user_group']['value'] == '04':
                continue
            if user_json['user_group']['value'] == '07':
                continue
            if user_json['user_group']['value'] == '08':
                continue
            first_name = ''
            if 'first_name' not in user_json:
                first_name = user_json['first_name']
            else:
                logging.warning("no first name given")
            last_name = user_json['last_name']
            name_field = first_name + ' ' + last_name
            if len(user_json['contact_info']['address']) > 0:
                for address in user_json['contact_info']['address']:
                    changed = False
                    if address['preferred']:
                        address_lines = []
                        for i in range(1, 6):
                            address_lines.append(address['line' + str(i)])
                        address_item = address['line1']
                        if (name_field == address_item):
                            address_lines.remove(address_item)
                            changed = True
                        else:
                            logging.info('no rearrangement of first line for user {}'.format(user))
                        if changed:
                            for i in range(0, 5):
                                if i < len(address_lines):
                                    address['line' + str(i + 1)] = address_lines[i]
                                else:
                                    address['line' + str(i + 1)] = ''
                        else:
                            logging.info('no address changes for user {}'.format(user))
                    else:
                        logging.debug('not preferred address')
            else:
                logging.warning('no addresses given for user {}'.format(user))
            update_user(user_json)
        else:
            logging.warning('could not retrieve user {}'.format(user))


def set_pin_pw_and_addresses_for_chunk(chunk, set_passphrase=False):
    users_already_set = load_identifier_list_of_type("users_already_set")
    for user in chunk:
        user = user.strip()
        if user == '':
            continue
        user_json = get_user(user)
        if user_json is not None:
            user_json = clean_title(user_json)
            if user_json['primary_id'] in users_already_set:
                logging.info("user {} already set.".format(user))
                continue
            first_name = ''
            try:
                first_name = user_json['first_name']
            except KeyError:
                logging.warning("no first name given")
            last_name = user_json['last_name']
            gender = user_json['gender']['value']
            title = user_json['user_title']['value']
            pin_new = False
            try:
                user_json['pin_number'] = calculate_standard_pin(user_json['birth_date'])
                pin_new = True
            except:
                logging.warning('could not load birthday for user {}'.format(user))
            passphrase = ''
            if set_passphrase:
                passphrase = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.ascii_lowercase +
                                                                  string.digits) for _ in range(12))
                user_json['password'] = passphrase
            email = ''
            if len(user_json['contact_info']['email']) > 0:
                for email in user_json['contact_info']['email']:
                    if email['preferred'] == True:
                        email = email['email_address']

            if len(user_json['contact_info']['address']) > 0:
                for address in user_json['contact_info']['address']:
                    changed = False
                    postal_code_changed = False
                    if address['preferred']:
                        address_lines = []
                        for i in range(1, 6):
                            address_lines.append(address['line' + str(i)])
                        for address_item in list(address_lines):
                            if address_item is None:
                                address_lines.remove(address_item)
                            elif postalcode_regexp.match(address_item):
                                changed = True
                                postal_code_changed = True
                                address['postal_code'] = postalcode_regexp.findall(address_item)[0]
                                address['city'] = postalcode_regexp.split(address_item)[2].strip()
                                address_lines.remove(address_item)
                            elif 'Herr' in address_item:
                                changed = True
                                address_lines.remove(address_item)
                            elif 'Frau' in address_item:
                                changed = True
                                address_lines.remove(address_item)
                        if postal_code_changed:
                            logging.info('rearranged postal code for user {}'.format(user))
                        else:
                            logging.info('no rearrangement of postal code for user {}'.format(user))
                        if changed:
                            for i in range(0, 5):
                                if i < len(address_lines):
                                    address['line' + str(i + 1)] = address_lines[i]
                                else:
                                    address['line' + str(i + 1)] = ''
                        else:
                            logging.info('no address changes for user {}'.format(user))
                    else:
                        logging.debug('not preferred address')
            else:
                logging.warning('no addresses given for user {}'.format(user))
            update_success = update_user(user_json)

            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update_success:
                logging.info('succesfully updated user | {} | {} | {} | {} | {} | {} | {} | {}'.format(user, gender,
                                                                                                       title,
                                                                                                       first_name,
                                                                                                       last_name,
                                                                                                       email, pin_new,
                                                                                                       passphrase))
            else:
                logging.warning('problem updating user {}'.format(user))
        else:
            logging.warning('could not retrieve user {}'.format(user))


def reset_password(project):
    table = read_password_list(project)
    for index, password in table.iterrows():
        user_id = password[' user_id '].strip()
        password_string = password[' password'].strip()
        user = get_user(user_id)
        if user is not None:
            user['password'] = password_string
            update_user(user)


def set_pin_and_password(project):
    users = load_identifier_list_of_type(project)
    logging.info('succesfully updated user | {} | {} | {} | {} | {} | {} | {}'.format('user_id', 'gender', 'title',
                                                                                      'first_name', 'last_name',
                                                                                      'passphrase', 'email'))
    for user_id in users:
        user = get_user(user_id)
        if user is not None:
            first_name = user['first_name']
            last_name = user['last_name']
            gender = user['gender']['value']
            title = user['user_title']['value']
            birth_date = user['birth_date']
            pin_code = calculate_standard_pin(birth_date)
            passphrase = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.ascii_lowercase +
                                                              string.digits) for _ in range(12))
            user['pin_number'] = pin_code
            user['password'] = passphrase
            email = ''
            if len(user['contact_info']['email']) > 0:
                for email in user['contact_info']['email']:
                    if email['preferred'] == True:
                        email = email['email_address']

            update_successful = update_user(user)
            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update_successful:
                logging.info(
                    'succesfully updated user | {} | {} | {} | {} | {} | {} | {}'.format(user_id, gender, title,
                                                                                         first_name, last_name,
                                                                                         passphrase, email))


def set_pin_old(project):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    users = load_identifier_list_of_type(project)
    logging.info('succesfully updated user | {} | {} | {} | {} | {} | {}'.format('user_id', 'gender', 'title',
                                                                                 'first_name', 'last_name', 'email'))
    for user_id in users:
        user = get_user(user_id)
        if user is not None:
            first_name = user['first_name']
            last_name = user['last_name']
            gender = user['gender']['value']
            title = user['user_title']['value']
            logging.debug(user)
            birth_date_parts = user['birth_date'].split('-')
            logging.debug(birth_date_parts)
            pin_code = birth_date_parts[2].replace('Z', '') + birth_date_parts[1] + birth_date_parts[0]
            user['pin_number'] = pin_code
            email = ''
            if len(user['contact_info']['emails']) > 0:
                for email in user['contact_info']['emails']:
                    if email['preferred'] == True:
                        email = email['email_address']
            update_successful = update_user(user)
            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update_successful:
                logging.info('succesfully updated user | {} | {} | {} | {} | {} | {}'.format(user_id, gender, title,
                                                                                             first_name, last_name,
                                                                                             email))


def set_hash(project):
    users = load_identifier_list_of_type(project)
    number_of_threads = 6
    length_of_chunks = math.ceil(len(users) / number_of_threads)
    list_chunks = list(chunks(users, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=set_hash_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def set_hash_for_chunk(chunk, test):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    for user_id in chunk:
        user = get_user(user_id)
        if user is not None:
            if 'last_name' in user:
                last_name = user['last_name']
            else:
                logging.warning('no last name given for user {}' + user_id)
                continue
            if 'birth_date' in user:
                birthday = user['birth_date']
            else:
                logging.warning('no birthday given for user {}' + user_id)
                continue
            if birthday is None:
                logging.warning('no birthday given for user {}' + user_id)
                continue
            if birthday == '':
                logging.warning('no birthday given for user {}' + user_id)
                continue
            else:
                birthday = birthday.replace('Z', '')

            string_to_hash = '{}-{}'.format(last_name, birthday)
            hash = hashlib.md5(string_to_hash.encode('utf-8')).hexdigest().upper()
            test_url = '{}users/{}?apikey={}'.format(alma_api_base_url, hash, api_key)
            test_hash = requests.get(url=test_url, headers={'Accept': 'application/json'})
            if test_hash.status_code == 200:
                logging.info('user {} has already id of type Dublettencheck'.format(user_id))
                continue
            hash_identifier = {
                "value": hash,
                "id_type": {
                    "value": "06",
                    "desc": "Dublettencheck"
                },
                "note": None,
                "status": "ACTIVE",
                "segment_type": "Internal"
            }
            user['user_identifier'].append(hash_identifier)
            update_successful = update_user(user)
            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update_successful:
                logging.info('successfully set hash {} for user {} '.format(hash, user_id))
        else:
            logging.warning('user {} not found'.format(user_id))


def delete_home_library(project):
    users = list_reader_service.load_identifier_list_of_type(project)
    # users = set_reader_service.collect_ids_from_set(set_id)
    # logging.info('retrieved {} user entries'.format(len(users)))
    # ids = []
    # for member in users:
        # ids.append(member['id'])
    logging.info('retrieved {} user ids'.format(len(users)))
    # list_reader_service.save_identifier_list_of_type('DeleteHomeLibrary', ids, 'users')
    # logging.info('saved {} user entries'.format(len(users)))
    number_of_threads = 1
    length_of_chunks = math.ceil(len(users) / number_of_threads)
    list_chunks = list(chunks(users, length_of_chunks))
    for chunk_index, chunk in enumerate(list_chunks):
        thread = Thread(target=delete_home_library_for_chunk,
                        args=(chunk, 'test'))
        thread.start()


def delete_home_library_for_chunk(chunk, test='test'):
    for user_id in chunk:
        user = alma_user_service.get_user(user_id=user_id)
        note_list = user['user_note']
        for note in note_list:
            if note['note_text'].startswith('HOME_LIBRARY: '):
                note_list.remove(note)
        alma_user_service.update_user(user)

