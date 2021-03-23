import logging

from model.LineChecker import LineChecker
from service.filter_chain_service import load_line_checker_list
from service.list_reader_service import load_identifier_list_of_type


def add_sys_list_checker(list_filter):
    checklist = load_identifier_list_of_type('package')
    line_checker = LineChecker(method_name='has_title_sys_id', checklist=checklist, mode='remove')
    list_filter.add_line_checker(line_checker)
    return list_filter


def add_id_checker(list_filter, action='append', list='springer_robotics_auswahl'):
    checklist = load_identifier_list_of_type(list)
    line_checker = LineChecker(method_name='contains', checklist=checklist, mode=action, field='020 ', format='aseq_L')
    list_filter.add_line_checker(line_checker)
    return list_filter


def run_project(project):
    # den Namen der Logdatei festlegen
    log_file = 'data/output/marc_processor_{}.log'.format(project)

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # den Logger konfigurieren (Dateinamen, Level)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=log_file, level=logging.DEBUG)

    logging.info('starting project {}'.format(project))

    # Lädt die enstprechende Filterkette
    list_filter = load_line_checker_list(project=project)

    # Löscht den Inhalt des temporären Ordners. Wenn dieser nicht existiert, wird er erzeugt.
    list_filter.clean_temp_folder(project)

    # Fügt den Filter hinzu, der prüft, ob die Sys-ID auf einer Pakete Liste enthalten ist.
    list_filter = add_sys_list_checker(list_filter)
    # list_filter = add_id_checker(list_filter=list_filter, action='append', list='springer_robotics_auswahl')

    # list_filter.test_field_values()

    # Filter anwenden
    list_filter.filter()
    # list_filter.remove_field(['001 ', '078u'])
    # list_filter.generate_field_value_list('540a ', False, format='marc')
    return list_filter


# Hauptstartpunkt. Python startet anhand dieser Zeilen das Skript. Muss am Ende stehen.
if __name__ == '__main__':
    # rojects = ['db', 'zsn', 'ebooks', 'db_lizenzfrei', 'zsn_lizenzfrei', 'ebooks_lizenzfrei', 'collections_from_db', 'zsn_ezb']
    projects = ['db_lizenzfrei']
    # projects = ['springer_all']
    for project in projects:
        list_filter = run_project(project=project)

        # aus der letzten temporären Datei wird die P2E-Datei erzeugt.
        list_filter.generate_p2e_file()

        # aus der letzten temporären Datei wird eine Liste der Feld-Werte erzeugt
        list_filter.generate_field_value_list(field='001 ', short=False)

    logging.info('finished')
