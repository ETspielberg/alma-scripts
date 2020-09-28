import os

import pandas as pd
import requests


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
    with open('data/output/all_records.xml', "w") as output_file:
        for file in os.listdir('data/temp/'):
            if file.startswith('ids_'):
                with open('data/temp/{}'.format(file), 'r') as input_file:
                    output_file.write(input_file.read())


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
