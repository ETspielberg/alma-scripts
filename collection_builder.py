import logging

from service import table_reader_service
from service.alma import alma_primo_service, alma_bib_service, alma_sru_service, alma_electronic_service
from service.table_reader_service import read_table, write_table

from utils import list_to_string


def collect_number_of_entries(project, table):
    """
    collects the number of titles found in Alma for the collection marks provided in an excel spreadsheet
    :param project: the name of the project.
    :param table: a dataframe with a column 'Kennzeichen' containing the collection marks
    :return: a dataframe with a column 'total_in_Alma' containing the number of hits
    """
    logging.info('project {}: starting collection of total number of entries'.format(project))

    # Neue Tabelle vorbereiten
    new_table_rows = []

    # Durch alle Einträge durchgehen
    for index, row in table.iterrows():
        logging.info(
            'project {}: processing mark {} of {}: {}'.format(project, str(index), str(len(table)), row['Kennzeichen']))

        # Anzahl der Treffer abrufen und in neue Spalte 'total_in_Alma' einfügen
        row['total_in_Alma'] = alma_sru_service.get_number_of_hits(project=project, field='local_field_912',
                                                                   search_term=row['Kennzeichen'])
        new_table_rows.append(row)

    # erweiterte Tabelle speichen und zurückgeben
    return write_table(project=project, rows=new_table_rows)


def collect_mms_ids(project, table):
    logging.info('project {}: starting collection of mms ids'.format(project))
    new_table_rows = []
    for index, row in table.iterrows():
        portfolios, packages = alma_primo_service.collect_portfolios_and_packages(project=project, field='lds03',
                                                                                  search_term=row['Kennzeichen'],
                                                                                  number_of_hits=row['total_in_Alma'])
        row['Paket_MMS'] = list_to_string(packages)
        row['Portfolio_MMS'] = list_to_string(portfolios)
        new_table_rows.append(row)

    # erweiterte Tabelle speichen und zurückgeben
    return write_table(project=project, rows=new_table_rows)


def retrieve_collection_ids(project, table):
    new_table_rows = []

    # Alle Pakete durchgehen
    for index, row in table.iterrows():
        collection_ids = alma_bib_service.retrieve_collection_id_for_mms_id(project=project, mms_ids=row['Paket_MMS'])
        row['Collection_IDs'] = list_to_string(collection_ids)
        new_table_rows.append(row)

    # erweiterte Tabelle speichen und zurückgeben
    return write_table(project=project, rows=new_table_rows)


def retrieve_portfolio_ids(project, table):
    new_table_rows = []

    # Alle Pakete durchgehen
    for index, row in table.iterrows():
        portfolio_ids = alma_bib_service.retrieve_portfolio_ids(project=project, mms_ids=row['Portfolio_MMS'])
        row['Portfolio_IDs'] = list_to_string(portfolio_ids)
        new_table_rows.append(row)

    # erweiterte Tabelle speichen und zurückgeben
    return write_table(project=project, rows=new_table_rows)


def retrieve_service_ids(project, table):
    new_table_rows = []

    # Alle Pakete durchgehen
    for index, row in table.iterrows():
        service_ids = alma_electronic_service.retrieve_service_ids(project=project,
                                                                   collection_ids=row['Collection_IDs'])
        row['Service_IDs'] = list_to_string(service_ids)
        new_table_rows.append(row)

    # erweiterte Tabelle speichen und zurückgeben
    return write_table(project=project, rows=new_table_rows)


def update_collections(project, table):
    # Alle Pakete durchgehen
    new_table_rows = []
    for index, row in table.iterrows():
        service_ids = []
        # Alle Collection-IDs durchgehen
        for collection_id in row['Collection_IDs'].split(';'):
            logging.info('updating collection {}'.format(collection_id))

            # Den Type auf selective package ändern
            success_type_change = alma_electronic_service.set_type_to_selective_package(project=project,
                                                                                         collection_id=collection_id)

            # Falls die Änderung erfolgreich war, einen Full-Text-Service hinzufügen
            if success_type_change:
                service_id = alma_electronic_service.add_active_full_text_service(project=project,
                                                                                  collection_id=collection_id)
                if service_id != '':
                    service_ids.append(service_id)
        row['Service_IDs'] = list_to_string(service_ids)
        new_table_rows.append(row)

    # erweiterte Tabelle speichen und zurückgeben
    return write_table(project=project, rows=new_table_rows)


def build_collections(project, table):
    # Alle Pakete durchgehen
    for index, row in table.iterrows():

        for portfolio_id in row['Portfolio_IDs'].split(';'):
            service_id = row['Service_IDs'][0]
            collection_id = row['Collection_IDs'][0]
            alma_electronic_service.add_portfolios_to_collection(project=project, portfolio_id=portfolio_id,
                                                                 service_id=service_id, collection_id=collection_id)


if __name__ == '__main__':
    # den Namen des Laufs angeben. Dieser definiert den name der Log-Datei und den Typ an Liste, die geladen wird.
    project = 'marks'

    new_run = False

    if new_run:
        # Excel-Tabelle mit Abrufkennzeichen einlesen
        table = read_table(project=project)
    else:
        table = table_reader_service.reload_table(project=project, index=0)

    # den Namen der Logdatei festlegen
    log_file = 'data/output/{}.log'.format(project)

    # den Logger konfigurieren (Dateinamen, Level)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=log_file, level=logging.INFO)

    # die Gesamtzahl der Einträge abrufen
    table = collect_number_of_entries(project=project, table=table)

    # die MMS-IDs hinzuschreiben
    # table = collect_mms_ids(project=project, table=table)

    # Die Collection-IDs der e-Kollektionen heraussammeln
    # table = retrieve_collection_ids(project=project, table=table)

    # table = retrieve_portfolio_ids(project=project, table=table)

    # table = update_collections(project=project, table=table)
    build_collections(project=project, table=table)
