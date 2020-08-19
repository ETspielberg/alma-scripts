import logging
import os

import requests

from service.list_reader_service import load_identifier_list_of_type

# Script that takes a list of Ids from the data/input folder and replaces the public record_type by staff ones

# base URL for the Alma API
alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def update_users():
    users = load_identifier_list_of_type('users')
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    for user in users:
        url = alma_api_base_url + 'users/{}?apikey={}'.format(user, api_key)
        get = requests.get(url=url, headers={'Accept': 'application/json'})
        get.encoding = 'utf-8'
        if get.status_code == 200:
            info = get.text
            if '"value":"PUBLIC","desc":"Public"' in info:
                payload = info.replace('"record_type":{"value":"PUBLIC","desc":"Public"}',
                                       '"record_type":{"value":"STAFF","desc":"Staff"}')
                update = requests.put(url=url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'})
                update.encoding = 'utf-8'
                if update.status_code == 200:
                    logging.info('succesfully updated user {}'.format(user))
                else:
                    logging.warning('problem updating user {}:{}'.format(user, update.text))
            else:
                logging.info('user {} not public'.format(user))
        else:
            logging.error(get.text)


def update_vendors(type):
    vendors = load_identifier_list_of_type('vendor_' + type)
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    for vendor_line in vendors:
        url = alma_api_base_url + 'acq/vendors/{}?accept=application/json&apikey={}'.format(vendor_line, api_key)
        get = requests.get(url=url, headers={'Accept': 'application/json'})
        get.encoding = 'utf-8'
        if get.status_code == 200:
            info = get.text
            payload = info.replace('"status":{"value":"ACTIVE","desc":"Active"}',
                                   '"status":{"value":"INACTIVE","desc":"Inactive"}')
            update = requests.put(url=url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'})
            update.encoding = 'utf-8'
            if update.status_code == 200:
                logging.info('succesfully updated vendor {}'.format(vendor_line))
            else:
                logging.warning('problem updating vendor {}:{}'.format(vendor_line, update.text))
        else:
            logging.error(get.text)


def deactivate_non_used_vendors(type):
    vendors = load_identifier_list_of_type('vendor_' + type)
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    for vendor_line in vendors:
        url = alma_api_base_url + 'acq/vendors/{}/po-lines?accept=application/json&apikey={}'.format(vendor_line, api_key)
        get_list = requests.get(url=url, headers={'Accept': 'application/json'})
        get_list.encoding = 'utf-8'
        if get_list.status_code == 200:
            if (get_list.json()['total_record_count'] == 0):
                url = alma_api_base_url + 'acq/vendors/{}?accept=application/json&apikey={}'.format(vendor_line, api_key)
                get = requests.get(url=url, headers={'Accept': 'application/json'})
                get.encoding = 'utf-8'
                if get.status_code == 200:
                    info = get.json()
                    if info['status']['value'] == 'ACTIVE':
                        info = get.text
                        payload = info.replace('"status":{"value":"ACTIVE","desc":"Active"}',
                                   '"status":{"value":"INACTIVE","desc":"Inactive"}')
                        update = requests.put(url=url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'})
                        update.encoding = 'utf-8'
                        if update.status_code == 200:
                            logging.info('succesfully updated vendor {}'.format(vendor_line))
                        else:
                            logging.error('problem updating vendor {}:{}'.format(vendor_line, update.text))
                    else:
                        logging.info('account {} is already inactive'.format(vendor_line))
                else:
                    logging.error(get.text)
            else:
                logging.warning('account {} still has {} po-lines'.format(vendor_line, get_list.json()['total_record_count']))
        else:
            logging.error(get_list.text)

def check_log(type):
    vendors = load_identifier_list_of_type('vendor_' + type)
    for vendor in vendors:
        logging.info(vendor)


if __name__ == '__main__':
    run_name = 'users'
    log_file = 'data/output/{}.log'.format(run_name)
    logging.basicConfig(filename=log_file, level=logging.DEBUG)
    update_users()
    # update_vendors('do')
    # deactivate_non_used_vendors(run_name)
    # check_log(run_name)


