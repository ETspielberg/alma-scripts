import os

import pandas as pd

input_folder = 'data/input/{}.xlsx'
output_base_folder = 'data/output/{}'

D_TYPES = {'Standort': object,
'Signatur': object,
'Titel': object,
'Vorgänger': object,
'Fortsetzung': object,
'Prägung': object,
'Teilangabe': object,
'Farbe': object,
'Einband': object,
'Bindung': object,
'Bindefolge': object,
'Buchbinder': object,
'Band': object,
'Jahr': object,
'Teil': object,
'Bemerkungen Intern': object,
'Bemerkungen Extern': object,
'Bemerkungen Alt': object,
'FF': object,
'Abbestellt': object,
'FS': object
           }


def read_table(project):
    path_to_file = input_folder.format(project)
    table = pd.read_excel(path_to_file, dtype=D_TYPES)
    return table


def write_coredata_table(rows, suffix):
    if len(rows) == 0:
        return None
    output_folder = output_base_folder.format('coredata')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_file = output_folder + '/{}.xlsx'.format('coredata_' + suffix)
    new_table = pd.DataFrame(rows)
    new_table.to_excel(output_file, index=False)
    return new_table

