import os

import pandas as pd

input_folder = 'data/input/{}.xlsx'
output_base_folder = 'data/output/{}'
temp_base_folder = 'data/temp/{}'


def read_table(project):
    path_to_file = input_folder.format(project)
    table = pd.read_excel(path_to_file, dtype={'Kennung': object, 'Paket_MMS': object, 'Portfolio_MMS': object})
    return table


def reload_table(project, index=0, temp=False):
    if temp:
        input_folder = temp_base_folder.format(project)
    else:
        input_folder = output_base_folder.format(project)
    input_file = input_folder + '/output_step_{}.xlsx'.format(index)
    table = pd.read_excel(input_file, dtype={'Kennung': object, 'Paket_MMS': object, 'Portfolio_MMS': object})
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
