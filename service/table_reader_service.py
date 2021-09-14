import os

import pandas as pd

input_folder = 'data/input/{}.xlsx'
input_folder_csv = 'data/input/{}.csv'
output_base_folder = 'data/output/{}'
temp_base_folder = 'data/temp/{}'

D_TYPES = {'Kennung': object,
           'Paket_MMS': object,
           'Portfolio_MMS': object,
           'Collection_IDs': object,
           'Code': object,
           'order_number': object,
           'number_of_items': int,
           'shipment_date': object,
           'arrival_date': object,
           'arrival_note': object,
           'Loksatznummer': object,
           'HT-Nummer': object,
           'OWN': object,
           'Feld 125b': object,
           'MMS-ID': object,
           'Holding-ID': object,
           'ADM+SEQ': object,
           'Priorität': object,
           'angelegt am': object,
           'Barcode': object,
           'Abholort': object,
           'UserGroup': object,
           'ItemId': object,
           'PrimaryIdentifier': object,
           'Benutzer_id': object,
           'log': object,
           ' user_id ': object,
           'gender': object,
           'title': object,
           'first_name': object,
           'last_name': object,
           'email': object,
           'pin set': object,
           ' password': object,
           "Autor/Herausgeber": object,
           "Strichcode": object,
           "Inventarnummer": object,
           "Nummer erhalten": object,
           "Bibliothek": object,
           "Bibliothekseinheit": object,
           "Temporäre Bibliothek": object,
           "Erstellungsdatum": object,
           "Änderungsdatum": object,
           "Prozesstyp": object,
           "An Bibliothek": object,
           "Voraussichtliche Eingangszeit": object,
           "In der Bibliothek": object,
           "Bei": object,
           "Ablaufdatum der Bereitstellung": object,
           "Fälligkeitsdatum": object,
           "Benötigt bis": object,
           "bis:": object,
           "Permanenter Standort": object,
           "Temporärer Standort": object,
           "Signatur": object,
           "Signaturtyp": object,
           "Zugangsnummer": object,
           "Temporäre Signatur": object,
           "Temporärer Signaturtyp": object,
           "Exemplarsignatur": object,
           "Exemplarsignaturtyp": object,
           "Status": object,
           "RFID-Sicherheitsstatus": object,
           "Rückgabe fällig": object,
           "Exemplar-Richtlinie": object,
           "Richtlinie für temporäre Exemplare": object,
           "Materialart": object,
           "Beschreibung": object,
           "Bestellungen": object,
           "Vormerkungen": object,
           "Peer Reviewed": object,
           "Bestands-ID": object,
           "Bestellnummer": object,
           "Bestell-Status": object,
           "ADM-Nummer+ Sequenz": object,
           "Trigger-RecKey": object,
           "Bearbeiter": object,
           "Abteilung": object,
           "Notiz-Text": object,
           "noch notwendig?": object,
           "Anmerkung": object,
           "Code alt": object,
           "Code neu": object,
           "Server": object,
           "Lokalsatznr": object,
           "OWN_1": object,
           "Feld 125_1": object,
           "Feld 125_2": object,
           "Feld 125_3": object,
           "Feld 125_4": object,
           "Feld 125_5": object,
           "Feld 125_6": object,
           "Feld 125_7": object,
           "Feld 125_8": object,
           "Feld 125_9": object,
           "Feld 125_10": object,
           "Feld 125_11": object,
           'Exemplarsignatur (neu)': object,
           'Fulfillment Note  (löschen)': object,
           'Item Id': object,
           'aktuelle Exemplarsignatur ': object,
           'korrekte Exemplarsignatur': object,
           'Holding-Signatur': object,
           "Type / Creator / Imprint": object,
           "Inventory Number": object,
           "Receiving Number": object,
           "Library": object,
           "Library Unit": object,
           "Temporary Library": object,
           "Creation Date": object,
           "Modification Date": object,
           "Process type": object,
           "To Library": object,
           "Expected Arrival Time": object,
           "At Library": object,
           "At": object,
           "On Hold Expiration Date": object,
           "Due Date": object,
           "Needed By": object,
           "Until": object,
           "Permanent Location": object,
           "Temporary Location": object,
           "Call Number": object,
           "Call Number Type": object,
           "Accession Number": object,
           "Temporary Call Number": object,
           "Temporary Call Number Type": object,
           "Item call number": object,
           "Item call number type": object,
           "RFID Security Status": object,
           "Due back": object,
           "Item Policy": object,
           "Temporary Item Policy": object,
           "Material Type": object,
           "Copy ID": object,
           "Description": object,
           "": object,
           "Orders": object,
           "Requests": object,
           "Item ID": object,
           "Holdings ID": object,
           "MMS ID": object,
           "GrundSchlüssel": object,
           "signatur": object,
           "Titel": object,
           "bubi": object,
           "loesch": object,
           "farbe": object,
           "pappe": object,
           "bindehin": object,
           "Bandangabe": object,
           "Sonderkosten": object
           }


def read_table(project):
    path_to_file = input_folder.format(project)
    table = pd.read_excel(path_to_file, dtype=D_TYPES)
    return table


def read_arrival_information_csv(project):
    path_to_file = input_folder_csv.format(project)
    columns = ['order_number', 'number_of_items', 'shipment_date', 'arrival_date', 'arrival_note']
    table = pd.read_csv(path_to_file, sep=';', names=columns, header=None, dtype=D_TYPES)
    return table


def reload_table(project, index=0, temp=False):
    if temp:
        input_folder = temp_base_folder.format(project)
    else:
        input_folder = output_base_folder.format(project)
    input_file = input_folder + '/output_step_{}.xlsx'.format(index)
    table = pd.read_excel(input_file, dtype=D_TYPES)
    return table


def read_transfer_table(transfer_project):
    columns = ['index', 'Loksatznummer', 'HT-Nummer', 'OWN', 'Feld 125b.1', 'Feld 125b.2', 'Feld 125b.3', 'Feld 125b.4',
               'Feld 125b.5', 'MMS-ID', 'Holding-ID', 'comment']
    path_to_file = input_folder.format(transfer_project.input_file)
    table = pd.read_excel(path_to_file, dtype=D_TYPES, names=columns)
    return table


def write_table(project, rows, index=0, temp=False):
    if temp:
        output_folder = temp_base_folder.format(project)
    else:
        output_folder = output_base_folder.format(project)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_file = output_folder + '/output_step_{}.xlsx'.format(index)
    new_table = pd.DataFrame(rows)
    new_table.to_excel(output_file)
    return new_table


def read_requests_table():
    path_to_file = input_folder_csv.format('offene_vormerkungen')
    table = pd.read_csv(path_to_file, dtype=D_TYPES, delimiter='|')
    return table


def read_sem_apps_table():
    path_to_file = input_folder_csv.format('SemAppAusleihen')
    table = pd.read_csv(path_to_file, dtype=D_TYPES, delimiter=',')
    return table


def read_user_list(project):
    path_to_file = input_folder_csv.format(project)
    columns = ['user_id']
    table = pd.read_csv(path_to_file, dtype=D_TYPES, delimiter=',', header=None, names=columns)
    return table


def read_password_list(project):
    path_to_file = input_folder_csv.format(project)
    table = pd.read_csv(path_to_file, dtype=D_TYPES, delimiter=';')
    return table


def read_alma_holdings_list(project):
    path_to_file = input_folder.format(project)
    table = pd.read_excel(path_to_file, dtype=D_TYPES)
    return table


def read_alma_items_export(project):
    path_to_file = input_folder.format(project)
    table = pd.read_excel(path_to_file, dtype=D_TYPES)
    return table


def read_poline_notes_list(project):
    columns = ['poline', 'note']
    path_to_file = input_folder_csv.format(project)
    table = pd.read_csv(path_to_file, header=None, dtype=D_TYPES, names=columns, delimiter=',')
    return table


def read_memolist_table():
    path_to_file = input_folder.format('Memoliste')
    return pd.read_excel(path_to_file, dtype=D_TYPES)


def write_memolist_table(rows, order_id):
    if len(rows) == 0:
        return None
    output_folder = output_base_folder.format('memolist')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_file = output_folder + '/{}.xlsx'.format(order_id)
    new_table = pd.DataFrame(rows)
    new_table.drop(columns=['Anmerkung', 'noch notwendig?'], inplace=True)
    new_table.to_excel(output_file, index=False)
    return new_table


def read_partners_recode_table():
    path_to_file = input_folder.format('recode_partners')
    return pd.read_excel(path_to_file, dtype=D_TYPES)
