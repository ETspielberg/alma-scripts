import json

import pandas as pd
import numpy as np
import requests
import requests_cache

try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup

# ruft die collection id für einen collection-Eintrag ab
def get_ezb_id(collection):
    # falls keine collection angegeben, ein leeres Feld zurückgeben
    if collection is '':
        return ''

    # die URL zur ezb bilden
    url = 'https://ezb.ur.de/api/collections/' + collection

    # Seite abrufen
    request = requests.get(url)

    # wenn die Abfrage erfolgreich war (status = 200) dann die Werte auslesen
    if request.status_code == 200:

        # den Inhalt der Seite (=request.content) mit BeutifulSoup
        # https://www.crummy.com/software/BeautifulSoup/bs4/doc/ einlesen
        parsed_html = BeautifulSoup(request.content, features="lxml")

        # im p-Tag den JSON-formatierten Inhalt einlesen. Da dort manchmal Hinweise stehen, das ganze in einen try-
        # catch-Block einfassen. Falls der Inhalt sich nicht als JSON einlesen lässt, wird ein leeres Feld
        # zurückgegeben. Ansonsten wird das Feld "coll_id" aus dem JSON-Teil ausgelesen und zurückgegeben
        try:
            json_object = json.loads(parsed_html.find("p").get_text())
            return json_object['coll_id']
        except:
            return ''

    # falls der Aufruf der Seite keinen Erfolg hatte (status is nicht 200) wird ein leeres Feld zurückgegeben.
    else:
        return ""


def collect_data(filename):
    # die einzelnen Abfragen werden gecached, damit nicht mehrfach die gleiche Abfrage durchgeführt wird
    requests_cache.install_cache('demo_cache')

    # der Pfad zu der Originaldatei, in diesem Fall im Unterordner data/input, ausgehend von dem Ort dieser Datei
    path = 'data/input/{}'.format(filename)

    # Lese die Datei ein, dabei aufpassen, dass die package:collection-Spalte als Text eingelesen wird
    df = pd.read_excel(path, dtype={'package:collection': str, 'zdb_id': str})

    # alle "Not a number" (nan) durch leere Textfelder ersetzen
    df = df.replace(np.nan, "", regex=True)

    # Liste der zu schreibenden Reihen vorbereiten
    extended_rows = []

    # alle Spalten abarbeiten
    for index, row in df.iterrows():
        # für jeden hundertsten Eintrag den aktuellen Stand auf der Kommandozeile angeben.
        if index % 100 == 0:
            print("processing line {} of {}".format(index, len(df)))

        # die ezb collection id abfragen durch Aufruf der oben definierten Funktion
        ezb_collection_id = get_ezb_id(row['package:collection'])

        # als neuen Wert in die Spalte "collection_id" eintragen
        row['collection_id'] = ezb_collection_id

        # diese Reihe der Liste der zu schreibenden Reihen anhängen
        extended_rows.append(row)

    # die Liste der zu schreibenden Reihen in ein Pandas-Dataframe umwandeln
    output_df = pd.DataFrame(extended_rows)

    # das Dataframe in eine Datei schreiben. diese heißt genauso wie die Ursprungsdatei, mit vorgehängtem "out_" und
    # befindet sich im Ordner data/output relativ zu dieser Datei
    output_df.to_excel('data/output/out_{}'.format(filename))


# python-Standard-Startpunkt für das Skript
if __name__ == '__main__':
    # der Dateiname der zu erweiternden Datei im Ordner data/input relativ zu dieser Datei
    filename = 'Migrationsdatei.xlsx'
    # obige Funktion aufrufen und Daten sammeln
    collect_data(filename)
