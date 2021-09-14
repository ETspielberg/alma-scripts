import logging
import os

import requests
from lxml import etree
from lxml.etree import Element

from service.alma import alma_electronic_service

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'
alma_api_key = os.environ['ALMA_SCRIPT_API_KEY']


def retrieve_collection_id_for_mms_id(project, mms_ids, library=None):
    collection_ids = []

    # Alle MMS-IDs durchgehen
    for mms_id in mms_ids.split(';'):

        logging.info('project {}: reading information for e-collection {}'.format(project, mms_id))

        # Die URL für die API zusammensetzen
        url = '{}bibs/{}/e-collections?apikey={}'.format(alma_api_base_url, mms_id, alma_api_key)

        # GET-Abfrage ausführen
        get = requests.get(url=url, headers={'Accept': 'application/json'})

        # Kodierung auf UTF-8 festsetzen
        get.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get.status_code == 200:

            # Den Inhalt der Antwort als Text auslesen
            info = get.json()

            try:
                for collection in info['electronic_collection']:
                    collection_id = collection['id']
                    if library is None:
                        collection_ids.append(collection_id)
                    else:
                        collection = alma_electronic_service.get_collection(collection_id=collection_id)
                        try:
                            if collection['library']['value'] == library:
                                collection_ids.append(collection_id)
                        except:
                            logging.warning(
                                'project {}: could not retrieve collection {}'.format(project, collection_id))
            except KeyError:
                logging.error('project {}: no collection found for mms_id {}'.format(project, mms_id))
    return list(dict.fromkeys(collection_ids))


def retrieve_portfolio_ids(project, mms_ids, library=None):
    portfolio_ids = []
    # Alle MMS-IDs durchgehen
    for mms_id in mms_ids.split(';'):

        logging.info('project {}: reading Information for portfolio {}'.format(project, mms_id))

        # Die URL für die API zusammensetzen
        url = '{}bibs/{}/portfolios?apikey={}'.format(alma_api_base_url, mms_id, alma_api_key)

        # GET-Abfrage ausführen
        get = requests.get(url=url, headers={'Accept': 'application/json'})

        # Kodierung auf UTF-8 festsetzen
        get.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if get.status_code == 200:

            # Den Inhalt der Antwort als Text auslesen
            info = get.json()

            try:
                for portfolio in info['portfolio']:
                    portfolio_id = portfolio['id']
                    if library is None:
                        portfolio_ids.append(portfolio_id)
                    else:
                        portfolio_json = alma_electronic_service.get_stand_alone_portfolio(portfolio_id=portfolio_id)
                        try:
                            if portfolio_json['library']['value'] == library:
                                portfolio_ids.append(portfolio_id)
                        except:
                            logging.error('project {}: could not retrieve portfolio {}'.format(project, portfolio_id))
            except KeyError:
                logging.error('project {}: no portfolio found for mms_id {}'.format(project, mms_id))

    return list(dict.fromkeys(portfolio_ids))


def retrieve_mms_id(ht_number):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}bibs?other_system_id={}&apikey={}'.format(alma_api_base_url, ht_number.upper(), api_key)
    try:
        response = requests.get(url=url, headers={'Accept': 'application/json'})
        response.encoding = 'utf-8'

        # Wenn die Abfrage erfolgreich war, das neue Bestellobjekt erzeugen
        if response.status_code == 200:
            json = response.json()
            return json['bib'][0]['mms_id']
        else:
            logging.warning('could not get MMS ID from API - response was {}'.format(response.status_code))
            return None
    except:
        logging.warning('could not get MMS ID from API - error upon connection')
        return None


def retrieve_holdings(mms_id, library=''):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}bibs/{}/holdings?&apikey={}'.format(alma_api_base_url, mms_id, api_key)
    try:
        response = requests.get(url=url, headers={'Accept': 'application/json'})
        response.encoding = 'utf-8'

        # Wenn die Abfrage erfolgreich war, das neue Bestellobjekt erzeugen
        if response.status_code == 200:
            json = response.json()
            holdings = []
            if 'holding' in json:
                if library == '':
                    holdings = json['holding']
                else:
                    for holding in json['holding']:
                        if holding['library']['value'] == library:
                            holdings.append(holding)
                        elif holding['library']['value'] == 'UNASSIGNED':
                            holdings.append(holding)
            return holdings
        else:
            logging.warning(
                'could not get holding from API - response was {}: {}'.format(response.status_code, response.text))
            return None
    except:
        logging.warning('could not get Holding from API - error upon connection:: {}')
        return None


def retrieve_holding_record(mms_id, holding_id):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}bibs/{}/holdings/{}?apikey={}'.format(alma_api_base_url, mms_id, holding_id, api_key)
    logging.debug('querying url: ' + url)
    try:
        response = requests.get(url=url, headers={'Accept': 'application/xml'})
        response.encoding = 'utf-8'

        # Wenn die Abfrage erfolgreich war, das neue Bestellobjekt erzeugen
        if response.status_code == 200:
            return etree.fromstring(response.content)
        else:
            logging.warning(
                'could not get holding from API - response was {}: {}'.format(response.status_code, response.text))
            return None
    except:
        logging.warning('could not get Holding from API - error upon connection:: {}')
        return None


def add_991s_to_holding_record(holding_record, comments):
    subfields = holding_record.xpath('.//datafield[@tag="991"]/subfield[@code="a"]/.')
    comment_fields = []
    updated = False
    for subfield in subfields:
        comment_fields.append(subfield.text)
    for comment in comments:
        if comment in comment_fields:
            continue
        updated = True
        datafield = build_991_field(comment)
        for child in holding_record:
            if child.tag == 'record':
                child.append(datafield)
    return updated


def add_signature_to_holding_record(mms_id, holding_id, holding_record, signature):
    subfields = holding_record.xpath('.//datafield[@tag="852"]/subfield[@code="h"]')
    if subfields:
        if len(subfields) == 1:
            if subfields[0].text != signature:
                subfields[0].text = signature
                update_holding_record(mms_id=mms_id, holding_id=holding_id, holding_record=holding_record)
            else:
                logging.info('holding signature already set to {}  | {} | {}'.format(signature, mms_id, holding_id))
        elif len(subfields == 0):
            logging.warning('no subfield h given | {} | {}'.format(mms_id, holding_id))
        else:
            logging.warning('more than one subfield h given | {} | {}'.format(mms_id, holding_id))
    else:
        logging.warning('no field 852h in holding | {} | {}'.format(mms_id, holding_id))
    return holding_record


def build_991_field(comment):
    datafield = Element('datafield', ind1=' ',
                        ind2=' ', tag='991')
    subfield = Element('subfield', code='a')
    subfield.text = comment
    datafield.append(subfield)
    return datafield


def update_holding_record(mms_id, holding_id, holding_record, ht_number=''):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    url = '{}bibs/{}/holdings/{}?apikey={}'.format(alma_api_base_url, mms_id, holding_id, api_key)
    try:
        response = requests.put(url=url, data=etree.tostring(holding_record),
                                headers={'Content-Type': 'application/xml', 'Accept': 'application/xml'})
        response.encoding = 'utf-8'

        # Wenn die Abfrage erfolgreich war, das neue Bestellobjekt erzeugen
        if response.status_code < 300:
            logging.info('successfully updated holding | {} | {} | {}'.format(mms_id, holding_id, ht_number))
            return True
        else:
            logging.warning(
                'could not get holding from API - response was {}: {}'.format(response.status_code, response.text))
            return False
    except:
        logging.warning('could not get Holding from API - error upon connection:: {}')
        return False
