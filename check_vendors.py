import os

import requests

from service.list_reader_service import load_identifier_list_of_type

# Script that takes a list of Ids from the data/input folder and replaces the public record_type by staff ones

if __name__ == '__main__':
    vendors = load_identifier_list_of_type('vendor')
    api_key = os.environ['ALMA_ACQ_API_KEY']
    total = 0
    not_in_alma = 0
    for vendor in vendors:
        url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/acq/vendors/{}?apikey={}'.format(vendor, api_key)
        get = requests.get(url=url, headers={'Accept': 'application/json'})
        get.encoding = 'utf-8'
        if get.status_code == 200:
            print('vendor {} found in alma'.format(vendor))
        else:
            not_in_alma = not_in_alma + 1
