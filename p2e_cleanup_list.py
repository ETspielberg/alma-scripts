import os

from LineChecker import LineChecker
from service.list_reader_service import load_identifier_list

output_dir = 'data/output/{}/'


# hängt den aktuellen Eintrag an die Ausgabedatei an.
def append_to_file(entry, project):
    """
    writes one entry to the output file
    :param entry: the entry to be written
    :param project: the project name used to determine the output folder and filename.
    """
    # Das Basisverzeichnis ist data/output relativ zum Verzeichnis dieser Datei.
    base_directory = output_dir.format(project)
    # Der Name der Ausgabedatei
    output_filename = project + '.txt'
    # Die Datei im "Anhängen"-Modus öffnen und die einzelnen Zeilen des Eintrags der Datei anhängen. Dann die Datei
    # schließen.
    with open(base_directory + output_filename, 'a+', encoding="utf8") as output_file:
        output_file.writelines(entry)
        output_file.close()


# prüft, ob die ausgabe-Datei bereits besteht und löscht sie gegebenenfalls.
def clean_ouput(project):
    """
    checks whether the output directory exists. if also the output file exists it is removed
    :param project: the name of the project
    """
    base_directory = output_dir.format(project)
    # Wenn das Verzeichnis nicht exisitert, wird es erstellt.
    if not os.path.exists(base_directory):
        os.makedirs(base_directory)
    # Der Name der Ausgabedatei.
    output_filename = project + '.txt'
    # Wenn die Datei bereits exisitiert, wird sie gelöscht und eine entsprechende Meldung ausgegeben.
    if os.path.exists(base_directory + output_filename):
        print('output file exists.')
        os.remove(base_directory + output_filename)


# Fügt je nach append_type und Ergbnis der Prüfung den Eintrag an die Ausgabe-Datei an.
def append_entry_if_necessary(entry, is_condition_fulfilled, append_type, project):
    """
    appends the entry (= list of lines) to the output file, depending on the append_type ('append' or 'remove') and the
    result of the LineChecker check.
    :param entry: the list of lines building one entry
    :param is_condition_fulfilled: the result of the LineChecker check
    :param append_type: 'append' (output file with matching condition) or 'remove' (output file with non-matching
    condition)
    :param project: the project name, used to determine the output folder and filename
    """
    if is_condition_fulfilled and append_type == 'append':
        append_to_file(entry, project)
    elif not is_condition_fulfilled and append_type == 'remove':
        append_to_file(entry, project)


def prepare_list(project, filename, line_checker, append_type):
    """
    does the magic and prepares the output lists from an input
    :param project: the name of the project, used to determine the output directory and filenames
    :param filename: the filename of the input file within the data/input folder
    :param line_checker: the LineChecker object used to check the conditions
    :param append_type: 'append' if the matching lines are to be collected, 'remove' if the matching lines are to be
    removed from the file
    """
    # Öffnet die Eingabedatei im Lesen-Modus.
    with open('data/input/' + filename, 'r', encoding="utf8") as input_file:

        # Lese die Zeilen in eine Liste.
        lines = input_file.readlines()
        # Erste ID als Startpunkt für den Vergleich einlesen .
        entry_id = lines[0][0:9]
        # Liste an zu schreibenden Zeilen pro Eintrag vorbereiten.
        entry = []
        # Die Bedinugung als Falsch intialisieren.
        is_condition_fulfilled = False
        # Den Counter für Treffer auf null setzen.
        matches = 0
        # alle Zeilen durchgehen.
        for index, line in enumerate(lines):
            # Falls die ID mit der vorherigen Zeile übereinstimmt, gehört sie zum selben Eintrag.
            if entry_id == line[0:9]:
                # Die Zeile wird der Eintragsliste hinzugefügt.
                entry.append(line)
                # Die vorgegebene Bedingung wird durch den LineChecker gepürft. Falls die Bedingung wahr ist,
                # wird dies in der Variablen 'is_condition_fulfilled' gespeichert und der Zähler für die Treffer
                # erhöht.
                if line_checker.check(line):
                    is_condition_fulfilled = True
                    matches += 1
            else:
                # Falls die ID nicht mit der voherigen Zeile übereinstimmt, beginnt ein neuer Eintrag.
                # Je nach Bedingungen des append_type ('append' oder 'remove') und dem Ergebnis des LineCheckers wird
                # der Eintrag der Ausgabedatei angehängt.
                append_entry_if_necessary(entry, is_condition_fulfilled, append_type, project)
                # Die Variable 'is_condition_fulfilled' wird wieder auf False zurückgesetzt.
                is_condition_fulfilled = False
                # Ein neue Liste an Zeilen für den neuen Eintrag wird mit der aktuellen Liste als erstem Eintrag
                # gebildet.
                entry = [line]
                # Die neue ID festhalten.
                entry_id = line[0:9]
                # Auch diese erste Zeile durch den LineChecker prüfen und ggf. die Variable 'is_condition_fulfilled' auf
                # 'wahr' setzen.
                if line_checker.check(line):
                    is_condition_fulfilled = True
                    matches += 1
        # Auch den letzten Eintrag der Ausgabedatei anhängen, wenn die Bedingugnen erfüllt sind.
        append_entry_if_necessary(entry, is_condition_fulfilled, append_type, project)
        # Die Anzahl der Treffer auf der Kommandozeile ausgeben
        print('found {} matches'.format(matches))
        # Die Inputdatei schließen.
        input_file.close()


# Hauptstartpunkt. Python startet anhand dieser Zeilen das Skript. Hier wird dann der Dateiname und die
# Zeilenzahl angepasst.
# Muss am Ende stehen.
if __name__ == '__main__':
    project = 'p2e_zsn'
    clean_ouput(project=project)

    file = 'p2e_zsn_only_ezb.txt'
    checker = LineChecker('contains')
    checker.field = '655e'
    # Wenn die EZB-Titel entfernt werden sollen, diese Bedingung verwenden:
    # condition = ['http://www.bibliothek.uni-regensburg.de/ezeit/?']

    # Wenn die DBIS-Titel gesammelt werden sollen, diese Bedingung verwenden:
    checker.checklist = ['http://dbis.uni-regensburg.de/frontdoor.php']

    # Wenn eine Reihe an IDs oder slesktionskennzeichen geprüft werden sollen, diese aus einer entsprechenden Datei
    # einlesen, diese Datei befindet sich im input-Ordner und trägt den Namen <ID-Type>_list.txt
    # checker.checklist = load_identifier_list(project, 'collection_ids')

    # 'remove' oder 'append'
    process_type = 'remove'

    # Arbeit machen!
    prepare_list(project=project, filename=file, line_checker=checker, append_type=process_type)
