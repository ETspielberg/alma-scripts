import os

import pandas as pd
import requests


def load_data(filename, library):
    temp_path = 'data/temp/ids_{}.txt'
    table = pd.read_excel('data/input/' + filename, dtype={'String for Activationform': object, '"Bestand UB Duisburg-Essen': object, 'klickbare URLs': object})
    for index, row in table.iterrows():
        try:
            if library in row['String for Activationform']:
                r = requests.get(row['klickbare URLs'])
                open(temp_path.format(index), 'wb').write(r.content)
        except:
            print('no url in table field given for collection')


def generate_output():
    with open('data/output/IZEXCLUDE_list.txt', "w") as output_file:
        for file in os.listdir('data/input/'):
            if file.startswith('ids_'):
                with open('data/temp/{}'.format(file), 'r') as input_file:
                    output_file.write(input_file.read())


def generate_folders():
    if not os.path.exists('data/output'):
        os.mkdir('data')
    if not os.path.exists('data/temp'):
        os.mkdir('data')


def generate_p2e_output():
    with open('data/output/p2e_list.txt', "w", encoding='utf-8') as output_file:
        for file in os.listdir('data/input/TL2_P2E'):
            print('loading file')
            with open('data/input/TL2_P2E/{}'.format(file), 'r', encoding='utf-8') as input_file:
                    output_file.write(input_file.read())


if __name__ == '__main__':
    # generate_p2e_output()
    # generate_folders()
    filename = 'Verwaltung-NZ.xlsx'
    load_data(filename)
    generate_output()
