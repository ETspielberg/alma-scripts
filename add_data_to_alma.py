import json
import logging
import os

import requests

from service import table_reader_service

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def add_arrival_information_to_order(project):
    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # Die csv-Datei einlesen
    table = table_reader_service.read_arrival_information_csv(project)

    # die Bestellnummer und die Bestellung initialisieren
    order_number = ''
    json_order = None

    # Alle Zeilen durchgehen
    for index, row in table.iterrows():
        # Wenn es sich um einen weiteren Eintrag zur aktuellen Bestellung handelt, den Notiztext den Notizen hinzufügen
        if order_number == row['order_number']:
            # Notiz-Text erzeugen
            note_text = '{} Ex am {} erhalten: {}'.format(row['number_of_items'], row['arrival_date'],
                                                          row['arrival_note'])

            # Notiz-Text als note-text-Objekt der note-Liste hinzufügen
            json_order['note'].append({"note_text": note_text})

        # Falls es sich um eine neue Bestellnummer handelt, zunächst die vorherige updaten, dann die neue abrufen und die Notzi hinzufügen
        else:
            # die Bestellung updaten (falls sie noch None ist, wird das dort abgefangen)
            update_order(json_order, order_number, api_key)

            # die neue Bestellnummer auslesen
            order_number = row['order_number']

            # die URL basteln
            url = '{}acq/po-lines/{}?apikey={}'.format(alma_api_base_url, order_number, api_key)

            # Die GET-Abfrage ausführen
            response = requests.get(url=url, headers={'Accept': 'application/json'})

            # Die Kodierung der Antwort auf UTF-8 festlegen
            response.encoding = 'utf-8'

            # Wenn die Abfrage erfolgreich war, das neue Bestellobjekt erzeugen
            if response.status_code == 200:
                json_order = response.json()
                logging.debug(json.dumps(json_order))

                # die entsprechende Notiz erzeugen und anhängen
                note_text = '{} Ex am {} erhalten: {}'.format(row['number_of_items'], row['arrival_date'],
                                                              row['arrival_note'])
                if json_order['note'] == '':
                    json_order['note'] = [{"note_text": note_text}]
                else:
                    json_order['note'].append({"note_text": note_text})

    # für die letzte Notiz ebenfalls das Update durchführen.
    update_order(json_order, order_number, api_key)


def update_order(order, order_number, api_key):
    if order is not None:
        url = '{}acq/po-lines/{}?apikey={}'.format(alma_api_base_url, order_number, api_key)

        # Update als PUT-Abfrage ausführen. URL ist die gleiche, Encoding ist wieder utf-8, Inhalt ist JSON
        logging.debug(json.dumps(order))
        update = requests.put(url=url, data=json.dumps(order),
                              headers={'Content-Type': 'application/json'})
        if update.status_code == 200:
            logging.info('added notes to order {}'.format(order_number))
        else:
            logging.warning('problem updating order {}: {}'.format(order_number, update.text))


# Haupt-Startpunkt eines jeden Python-Programms.
if __name__ == '__main__':
    # den Namen des Laufs angeben. Dieser definiert den name der Log-Datei und den Typ an Liste, die geladen wird.
    run_name = 'LBA-234'

    # den Namen der Logdatei festlegen
    log_file = 'data/output/{}.log'.format(run_name)

    # den Logger konfigurieren (Dateinamen, Level)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=log_file, level=logging.DEBUG)

    # welcher Lauf gestartet werden soll. Zum Aufrufen einer Funktion den entsprechenden Aufruf durch Entfernen des '#'
    # aktivieren

    add_arrival_information_to_order(run_name)
