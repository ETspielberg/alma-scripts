import json

from model.TransferProject import TransferProject


def load_project(project_id):
    path_to_file = 'transfers/transfer_{}.json'.format(project_id)
    with open(path_to_file) as json_file:
        transfer_json = json.load(json_file)
        json_file.close()
        transfer_project = TransferProject(**transfer_json)
        return transfer_project