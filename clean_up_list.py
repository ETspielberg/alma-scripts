import pandas as pd

from service import list_reader_service



def with_set():
    marks = list_reader_service.load_identifier_list_of_type("p2e")
    marks = list(dict.fromkeys(marks))
    list_reader_service.save_identifier_list_of_type('p2e', marks, 'p2e')


def with_dataframe():
    columns = ['ids', 'type']
    table = pd.read_csv('data/input/p2e_list.txt', sep=',', names=columns, header=None, dtype={'ids': object, 'type': object})
    print(table)
    table.drop_duplicates(subset=['ids'], keep='first', inplace=True)
    table.to_csv('data/output/p2e/p2e_list.txt', index=False, header=False)


if __name__ == '__main__':
    #with_set()
    with_dataframe()



