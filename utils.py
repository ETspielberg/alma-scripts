
def list_to_string(id_list):
    '''
    writes a list of identifiers as semicolon separated list into a single string
    :param id_list: a list of identifiers
    :return: a string, with the identifiers separated by ';'
    '''
    if len(id_list) == 0:
        return ''
    else:
        string = id_list.pop()
        while len(id_list) > 0:
            string = string + ';' + id_list.pop()
        return string