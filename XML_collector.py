import os
from xml.etree import ElementTree as ET
import pandas as pd
import numpy as np

input_folder = 'data/input/{}.xlsx'


def generate_output(project):
    path_to_file = input_folder.format(project)
    ET.register_namespace('', "http://www.loc.gov/MARC21/slim")
    table = pd.read_excel(path_to_file, dtype={'eISBN': object, 'URL': object, 'code': object})
    root = ET.Element('record')
    output_file = 'data/output/IZEXCLUDE_list.txt'
    for file in os.listdir('Y:\Dezernat 3 - Medienbearbeitung\E-Books\Metadaten\Benjamins\marcxml'):
        code = file.replace('.xml', '').replace('.', ' ').upper()


        tree = ET.parse('Y:\Dezernat 3 - Medienbearbeitung\E-Books\Metadaten\Benjamins\marcxml\{}'.format(file))
        record = tree.getroot()
        try:
            index = table[table.code == code].index[0]
            url = table.iloc[index][1]
            url_element = ET.SubElement(record, 'datafield')
            url_element.set('tag', '856')
            url_element.set('ind1', '4')
            url_element.set('ind2', '0')
            subfield_element = ET.SubElement(url_element, 'subfield')
            subfield_element.set('code', 'u')
            subfield_element.text = url
        except IndexError:
            print('no url found for file ' + file)
        root.append(record)
    ET.ElementTree(root).write(output_file, xml_declaration=True, encoding="utf-8", method="xml")


if __name__ == '__main__':
    project = 'benjamins'
    generate_output(project)
