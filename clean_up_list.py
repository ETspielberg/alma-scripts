from service import list_reader_service

if __name__ == '__main__':
    marks = list_reader_service.load_identifier_list_of_type("p2e")
    marks = list(dict.fromkeys(marks))
    list_reader_service.save_identifier_list_of_type('p2e', marks, 'p2e')
