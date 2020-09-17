
# Hauptstartpunkt. Python startet anhand dieser Zeilen das Skript. Hier wird dann der Dateiname und die
# Zeilenzahl angepasst.
# Muss am Ende stehen.
from model.LineChecker import LineChecker
from service.filter_chain_service import load_line_checker_list
from service.list_reader_service import load_identifier_list_of_type


def add_sys_list_checker(list_filter):
    checklist = load_identifier_list_of_type('package')
    line_checker = LineChecker(method_name='has_title_sys_id', checklist=checklist, mode='remove')
    list_filter.add_line_checker(line_checker)
    return list_filter

if __name__ == '__main__':
    project = 'ebooks'
    list_filter = load_line_checker_list(project=project)
    list_filter.clean_temp_folder(project)
    list_filter = add_sys_list_checker(list_filter)
    # list_filter.filter()
    # list_filter.generateP2EFile(record_type='portfolio')
    # list_filter.generateFieldValueList(field='001', short=False)
    list_filter.generateMarksFile()
    print('finished')
