import os


def load_identifier_list(provider):
    path_to_file = os.getcwd() + '/data/input/' + provider + '/isbn_list.txt'
    with open(path_to_file) as f:
        isbns = f.readlines()
        f.close()
        # remove whitespace characters like `\n` at the end of each line
    return [x.strip() for x in isbns]


def save_identifier_list(provider, isbns):
    path_to_file = os.getcwd() + 'data/output/' + provider + '/isbn_list.txt'
    if not os.path.exists(path_to_file):
        os.makedirs(path_to_file)
    with open(path_to_file, 'w') as list_file:
        for isbn in isbns:
            list_file.write(isbn + '\n')
        list_file.close()


def load_identifier_list_of_type(identifier_type):
    path_to_file = os.getcwd() + '/data/input/' + identifier_type + '_list.txt'
    with open(path_to_file) as f:
        isbns = f.readlines()
        f.close()
        # remove whitespace characters like `\n` at the end of each line
    return [x.strip() for x in isbns]