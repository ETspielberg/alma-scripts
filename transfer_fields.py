import logging
import os
from time import sleep
from lxml import etree

import requests
import numpy as np
from lxml.etree import Element

from service import transfer_service, table_reader_service

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1'


def get_mms_ids(table):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    mms_ids = []
    for index, row in table.iterrows():
        sleep(0.5)
        ht_number = row['HT-Nummer'].strip().upper()
        url = '{}/bibs?other_system_id={}&apikey={}'.format(alma_api_base_url, ht_number, api_key)
        try:
            response = requests.get(url=url, headers={'Accept': 'application/json'})
            response.encoding = 'utf-8'

            # Wenn die Abfrage erfolgreich war, das neue Bestellobjekt erzeugen
            if response.status_code == 200:
                json = response.json()
                mms_ids.append(json['bib'][0]['mms_id'])
            else:
                mms_ids.append('')
                logging.warning('could not get MMS ID from API - response was not 200')
        except:
            mms_ids.append('')
            logging.warning('could not get MMS ID from API - error upon connection')
    table['MMS-ID'] = np.array(mms_ids)
    return table


def get_holding_ids(table):
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    holding_ids = []
    for index, row in table.iterrows():
        mms_id = row['MMS-ID'].strip()
        if mms_id == '':
            holding_ids.append('')
            logging.warning('no MMS ID given for ' + row['HT-Nummer'])
            continue
        sub_library = row['OWN'].strip().upper()
        url = '{}/bibs/{}/holdings?apikey={}'.format(alma_api_base_url, mms_id, api_key)
        try:
            response = requests.get(url=url, headers={'Accept': 'application/json'})
            response.encoding = 'utf-8'

            # Wenn die Abfrage erfolgreich war, das neue Bestellobjekt erzeugen
            if response.status_code == 200:
                json = response.json()
                holdings_list = json['holding']
                found = False
                logging.debug('checking for library ' + sub_library)
                logging.debug('found libraries:')
                for holding in holdings_list:
                    library = holding['library']['value']
                    logging.debug(library)
                    if library == sub_library:
                        holding_ids.append(holding['holding_id'])
                        found = True
                        break
                if not found:
                    holding_ids.append('')
                    logging.warning('could not get holding ID from API - library not found in holding list')
            else:
                holding_ids.append('')
                logging.warning('could not get holding ID from API - response was not 200')
        except:
            holding_ids.append('')
            logging.warning('could not get MMS ID from API - error upon connection')
    table['Holding-ID'] = np.array(holding_ids)
    return table


def transfer_field(table, project):
    comment = []
    api_key = os.environ['ALMA_SCRIPT_API_KEY']
    source_fields = ['Feld 125b.1', 'Feld 125b.2', 'Feld 125b.3', 'Feld 125b.4', 'Feld 125b.5']
    for index, row in table.iterrows():
        contents = []
        mms_id = row['MMS-ID'].strip()
        if mms_id == '':
            comment.append('')
            logging.warning('no MMS ID given for ' + row['HT-Nummer'])
            continue
        holding_id = row['Holding-ID'].strip()
        url = '{}/bibs/{}/holdings/{}?apikey={}'.format(alma_api_base_url, mms_id, holding_id, api_key)
        print(url)
        try:
            # response = requests.get(url=url, headers={'Accept': 'application/xml'})
            # response.encoding = 'utf-8'

            # Wenn die Abfrage erfolgreich war, das neue Bestellobjekt erzeugen
            # if response.status_code == 200:
                # response_xml = etree.fromstring(response.content)
                for field in source_fields:
                    field_value = row[field]
                    if type(field_value) == str:
                        print(field_value)
                        datafield = Element('datafield', ind1=project.target_indicator_1,
                                            ind2=project.target_indicator_2, tag=project.target_field)
                        subfield = Element('subfield', code=project.target_subfield)
                        subfield.text = field_value
                        datafield.append(subfield)
                        # datafield = construct_element(field_value, project)
                        # print(etree.tostring(datafield, pretty_print=True))



        except:
            comment.append('could not update record - error upon connection')
            logging.warning('could not update record - error upon connection')
    # table['Kommentar'] = np.array(comment)
    return table

def construct_element(field_text, project):
    datafield = Element('datafield', ind1=project.target_indicator_1, ind2=project.target_indicator_2, tag=project.target_field)
    subfield = Element('subfield', code=project.target_subfield)
    subfield.text=field_text
    datafield.append(subfield)
    return datafield


def run_project(project):
    log_file = 'data/output/transfer_{}.log'.format(project)

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # den Logger konfigurieren (Dateinamen, Level)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=log_file, level=logging.DEBUG)

    logging.info('starting project {}'.format(project))

    transfer_project = transfer_service.load_project(project)
    table = table_reader_service.read_transfer_table(transfer_project)
    # table = get_mms_ids(table)
    # table = get_holding_ids(table)
    table = transfer_field(table, project)
    table.to_excel('result.xlsx')


if __name__ == '__main__':
    projects = ['field_125']
    for project in projects:
        list_filter = run_project(project=project)

        # aus der letzten temporären Datei wird die P2E-Datei erzeugt.
        # list_filter.generate_p2e_file()

        # aus der letzten temporären Datei wird eine Liste der Feld-Werte erzeugt
        # list_filter.generate_field_value_list(field='001 ', short=False, format='')

    logging.info('finished')
