import logging
import os

import requests

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def collect_ids_from_set(set_id, listname='id'):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    limit = 100
    offset = 0
    logging.info('collecting members {} to {}'.format(offset, offset + limit))
    url = '{}conf/sets/{}/members?apikey={}&limit={}&offset={}'.format(alma_api_base_url, set_id, api_key, limit, offset)
    get = requests.get(url=url, headers={'Accept': 'application/json'})
    get.encoding = 'utf-8'
    if get.status_code == 200:
        members = get.json()
        number_of_members = members['total_record_count']
        if number_of_members == 0:
            logging.warning('set {} is empty'.format(set_id))
            return []
        all_members = members['member']
        if all_members == 0:
            logging.warning('set {} contains no members'.format(set_id))
            return all_members
        while offset < number_of_members:
            offset = offset + limit
            logging.info('collecting members {} to {}'.format(offset, offset + limit))
            url = '{}conf/sets/{}?apikey={}&limit={}&offset={}'.format(alma_api_base_url, set_id, api_key, limit,
                                                                       offset)
            get = requests.get(url=url, headers={'Accept': 'application/json'})
            get.encoding = 'utf-8'
            if get.status_code == 200:
                if len(members['member']) > 0:
                    logging.info('adding {} members'.format(len(members['member'])))
                    all_members = all_members + members['member']
                else:
                    logging.warning('set {} contains no members with limit {} and offset {}: {}'.format(set_id, limit, offset, get.text))
            else:
                logging.warning('could not retrieve set members {} to {} for set {}'.format( limit, limit + offset, set_id))
        logging.info('successfully collected {} members'.format(len(all_members)))
        return all_members
    else:
        logging.warning('could not retrieve set with set ID {}\n{}'.format(set_id, get.text))
        return None
