
# Hauptstartpunkt. Python startet anhand dieser Zeilen das Skript. Hier wird dann der Dateiname und die
# Zeilenzahl angepasst.
# Muss am Ende stehen.
from model.LineChecker import LineChecker
from service.filter_chain_service import load_line_checker_list
from service.list_reader_service import load_identifier_list_of_type

if __name__ == '__main__':

    project = 'safari'
    list_filter = load_line_checker_list(project=project)
    # checklist = load_identifier_list_of_type('package')
    # line_checker = LineChecker(method='has_title_sys_id', checklist=checklist, mode='remove')
    # list_filter.add_line_checker(line_checker)
    list_filter.filter()
    # list_filter.generateP2EFile(record_type='portfolio')
    print('finished')
