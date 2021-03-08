import os

import logging

def load_identifier_list(provider):
    path_to_file = os.getcwd() + '/data/input/' + provider + '/isbn_list.txt'
    with open(path_to_file, encoding='utf-8') as f:
        isbns = f.readlines()
        f.close()
        # remove whitespace characters like `\n` at the end of each line
    return [x.strip() for x in isbns]


def save_identifier_list(provider, isbns):
    path_to_file = os.getcwd() + 'data/output/' + provider + '/isbn_list.txt'
    if not os.path.exists(path_to_file):
        os.makedirs(path_to_file)
    with open(path_to_file, 'w', encoding='utf-8') as list_file:
        for isbn in isbns:
            list_file.write(isbn + '\n')
        list_file.close()


def save_identifier_list_of_type(provider, identifier, list_type):
    path_folder = 'data/output/{}'.format(provider)
    if not os.path.exists(path_folder):
        os.makedirs(path_folder)
    path_to_file = path_folder + '/{}_list.txt'.format(list_type)
    with open(path_to_file, 'w', encoding='utf-8') as list_file:
        for isbn in identifier:
            list_file.write(isbn + '\n')
        list_file.close()


def load_identifier_list_of_type(identifier_type):
    path_to_file = os.getcwd() + '/data/input/' + identifier_type + '_list.txt'
    logging.info('loading file {}'.format(path_to_file))
    with open(path_to_file, encoding='utf-8') as f:
        isbns = f.readlines()
        f.close()
        # remove whitespace characters like `\n` at the end of each line
    return [x.strip() for x in isbns]


def load_ids_from_file(path_to_file):
    with open(path_to_file, encoding='utf-8') as f:
        ids = f.readlines()
        f.close()
        # remove whitespace characters like `\n` at the end of each line
    return [x.strip() for x in ids]
