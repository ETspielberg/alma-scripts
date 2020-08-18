import os

import requests

from service.list_reader_service import load_identifier_list_of_type

# Script that takes a list of Ids from the data/input folder and replaces the public record_type by staff ones

def update_users():
    users = load_identifier_list_of_type('users')
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    for user in users:
        url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/users/{}?accept=application/json&apikey={}'.format(
            user, api_key)
        get = requests.get(url=url, headers={'Accept': 'application/json'})
        get.encoding = 'utf-8'
        if get.status_code == 200:
            info = get.text
            payload = info.replace('"record_type":{"value":"PUBLIC","desc":"Public"}',
                                   '"record_type":{"value":"STAFF","desc":"Staff"}')
            update = requests.put(url=url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'})
            update.encoding = 'utf-8'
            if update.status_code == 200:
                print('succesfully updated user {}'.format(user))
            else:
                print('problem updating user {}:{}'.format(user, update.text))
        else:
            print(get.text)


def update_vendors(type):
    vendors = load_identifier_list_of_type('vendor_' + type)
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    for vendor_line in vendors:
        url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/acq/vendors/{}?accept=application/json&apikey={}'.format(
        vendor_line, api_key)
        get = requests.get(url=url, headers={'Accept': 'application/json'})
        get.encoding = 'utf-8'
        if get.status_code == 200:
            info = get.text
            payload = info.replace('"status":{"value":"ACTIVE","desc":"Active"}',
                                   '"status":{"value":"INACTIVE","desc":"Inactive"}')
            update = requests.put(url=url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'})
            update.encoding = 'utf-8'
            if update.status_code == 200:
                print('succesfully updated vendor {}'.format(vendor_line))
            else:
                print('problem updating vendor {}:{}'.format(vendor_line, update.text))
        else:
            print(get.text)


def deactivate_non_used_vendors(type):
    vendors = load_identifier_list_of_type('vendor_' + type)
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    for vendor_line in vendors:
        url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/acq/vendors/{}/po-lines?accept=application/json&apikey={}'.format(
            vendor_line, api_key)
        get_list = requests.get(url=url, headers={'Accept': 'application/json'})
        get_list.encoding = 'utf-8'
        if get_list.status_code == 200:
            if (get_list.json()['total_record_count'] == 0):
                url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/acq/vendors/{}?accept=application/json&apikey={}'.format(
                    vendor_line, api_key)
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
                            print('succesfully updated vendor {}'.format(vendor_line))
                        else:
                            print('problem updating vendor {}:{}'.format(vendor_line, update.text))
                    else:
                        print('account {} is already inactive'.format(vendor_line))
                else:
                    print(get.text)
            else:
                print('account {} still has {} po-lines'.format(vendor_line, get_list.json()['total_record_count']))
        else:
            print(get_list.text)


if __name__ == '__main__':
    # update_users()
    # update_vendors('do')
    deactivate_non_used_vendors('no_invoices')

