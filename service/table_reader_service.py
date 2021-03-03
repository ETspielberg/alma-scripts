import logging
import os

import pandas as pd

input_folder = 'data/input/{}.xlsx'
input_folder_csv = 'data/input/{}.csv'
output_base_folder = 'data/output/{}'
temp_base_folder = 'data/temp/{}'

D_TYPES = {'Kennung': object,
           'Paket_MMS': object,
           'Portfolio_MMS': object,
           'Collection_IDs': object,
           'Code': object,
           'order_number': object,
           'number_of_items': int,
           'shipment_date': object,
           'arrival_date': object,
           'arrival_note': object,
           'Loksatznummer': object,
           'HT-Nummer': object,
           'OWN': object,
           'Feld 125b': object,
           'MMS-ID': object,
           'Holding-ID': object,
           'ADM+SEQ': object,
           'Priorität': object,
           'angelegt am': object,
           'Barcode': object,
           'Abholort': object,
           'UserGroup': object,
           'ItemId': object,
           'PrimaryIdentifier': object
           }


def read_table(project):
    path_to_file = input_folder.format(project)
    table = pd.read_excel(path_to_file, dtype=D_TYPES)
    return table


def read_arrival_information_csv(project):
    path_to_file = input_folder_csv.format(project)
    columns = ['order_number', 'number_of_items', 'shipment_date', 'arrival_date', 'arrival_note']
    table = pd.read_csv(path_to_file, sep=',', names=columns, header=None, dtype=D_TYPES)
    logging.debug(table)
    return table


def reload_table(project, index=0, temp=False):
    if temp:
        input_folder = temp_base_folder.format(project)
    else:
        input_folder = output_base_folder.format(project)
    input_file = input_folder + '/output_step_{}.xlsx'.format(index)
    table = pd.read_excel(input_file, dtype=D_TYPES)
    return table


def read_transfer_table(transfer_project):
    columns = ['index', 'Loksatznummer', 'HT-Nummer', 'OWN', 'Feld 125b.1', 'Feld 125b.2', 'Feld 125b.3', 'Feld 125b.4', 'Feld 125b.5', 'MMS-ID', 'Holding-ID', 'comment']
    path_to_file = input_folder.format(transfer_project.input_file)
    table = pd.read_excel(path_to_file, dtype=D_TYPES, names=columns)
    return table


def write_table(project, rows, index=0, temp=False):
    if temp:
        output_folder = temp_base_folder.format(project)
    else:
        output_folder = output_base_folder.format(project)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_file = output_folder + '/output_step_{}.xlsx'.format(index)
    new_table = pd.DataFrame(rows)
    new_table.to_excel(output_file)
    return new_table


def read_requests_table():
    path_to_file = input_folder_csv.format('offene_vormerkungen')
    table = pd.read_csv(path_to_file, dtype=D_TYPES, delimiter='|')
    return table


def read_sem_apps_table():
    path_to_file = input_folder_csv.format('offene_vormerkungen')
    table = pd.read_csv(path_to_file, dtype=D_TYPES, delimiter=',')
    return table

