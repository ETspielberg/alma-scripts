import os

import pandas as pd
import requests

from model.LineChecker import LineChecker
from service import list_reader_service


def load_data(filename, library):
    temp_path = 'data/temp/ids_{}.txt'
    table = pd.read_excel(filename, dtype={'String for Activationform': object, 'Inventory UB Duisburg-Essen': object, 'URL zur ID-Liste': object})
    for index, row in table.iterrows():
        try:
            if library in row['String for Activationform']:
                r = requests.get(row['URL zur ID-Liste'])
                open(temp_path.format(index), 'wb').write(r.content)
        except:
            print('no url in table field given')


def generate_output():
    with open('data/output/alle_ids.txt', "w") as output_file:
        for file in os.listdir('data/temp/'):
            if file.startswith('ids_'):
                with open('data/temp/{}'.format(file), 'r') as input_file:
                    output_file.write(input_file.read())

def generate_aleph_loader_file(master_file):
    ht_numbers = list_reader_service.load_ids_from_file('data/output/alle_ids.txt')
    line_checker = LineChecker(method='part_on_checklist', checklist=ht_numbers, field='001 ')
    with open('data/input/' + master_file, 'r', encoding="utf8") as input_file:
        lines = input_file.readlines()
        aleph_id = 0;
        for index, line in enumerate(lines):
            if line_checker.check(line):
                generate_output_line(line, aleph_id)
                aleph_id += 1
        input_file.close()



def generate_folders():
    if not os.path.exists('data/output'):
        os.mkdir('data')
    if not os.path.exists('data/temp'):
        os.mkdir('data')


if __name__ == '__main__':
    generate_folders()
    filename = 'Verwaltung-NZ Liste der Kollektionen mit Downloadlinks.xlsx'
    library = 'UB_DuE'
    load_data(filename, library)
    generate_output()
