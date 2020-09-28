import pandas as pd

from service import list_reader_service


def generate_output(input_file, project):
    marks = list_reader_service.load_identifier_list_of_type(project)
    file = open('data/input/{}'.format(input_file), 'r', encoding='utf-8').read()
    table = []
    for mark in marks:
        entry = {}
        entry['mark'] = mark
        entry['count'] = file.count(mark)
        table.append(entry)
    output_file = 'data/output/{}_out.xlsx'.format(project)
    new_table = pd.DataFrame(table)
    new_table.to_excel(output_file)


if __name__ == '__main__':
    input_file = 'p2e_zsn_sort.lst'
    project = 'marks_zsn'
    generate_output(input_file, project)
