import json

from model.LineChecker import LineChecker
from model.ListFilter import ListFilter


def load_line_checker_list(project):
    """loads a project by the provided ID"""
    path_to_file = 'chains/filter_chain_{}.json'.format(project)
    with open(path_to_file) as json_file:
        chain_json = json.load(json_file)
        json_file.close()
        file = chain_json['file']
        list_filter = ListFilter(project=project, filename=file)
        for line_checker in chain_json['lineCheckers']:
            list_filter.add_line_checker(LineChecker(method_name=line_checker['method_name'],
                                                     checklist=line_checker['checklist'],
                                                     field=line_checker['field'], position=line_checker['position'],
                                                     mode=line_checker['mode']))
    return list_filter
