import os
import re
import logging

from model.LineChecker import LineChecker

output_dir = 'data/output/{}'

logging.basicConfig(filename='data/output/list_filter.log', level=logging.DEBUG)


class ListFilter:

    def __init__(self, project, filename, line_checkers=None, record_type='portfolio'):
        self._filename = filename
        self._project = project
        if line_checkers is None:
            self._line_checkers = []
        else:
            self._line_checkers = line_checkers
        self.test_ordering()
        self._record_type = record_type

    # überprüft, ob die Einträge in einer Datei zusammenhängend sind.
    def test_ordering(self):
        """
        terst whether the ordering of the fields is ok. fields with one identifier are to be close together without other lines in between
        :return: True, if the file is well ordered
        """
        # öffne die input-Datei
        with open('data/input/' + self._filename, 'r', encoding="utf8") as input_file:

            # Lese die Zeilen in eine Liste.
            lines = input_file.readlines()
            entry_id = lines[0][0:9]
            testset = set(entry_id)
            is_well_ordered = True
            for index, line in enumerate(lines):
                entry_new_id = line[0:9]
                if len(entry_new_id) == 1:
                    continue
                if entry_id == entry_new_id:
                    continue
                else:
                    if entry_new_id in testset:
                        logging.warning('reoccuring entry ID: ' + entry_new_id)
                        is_well_ordered = False
                        entry_id = entry_new_id
                    else:
                        testset.add(entry_new_id)
                        entry_id = entry_new_id
                        continue
            return is_well_ordered

    # Löscht temporäre Dateien eines vorherigen Laufes
    def clean_temp_folder(self, project):
        """
        checks whether temporary and output folder exists and creates them if necessary. Also removes files from previous runs.
        :param project:
        :return:
        """
        temp_dir = 'data/temp/{}'.format(project)
        temp_file = 'data/temp/{}/step_0.txt'.format(project)
        if not os.path.exists(temp_dir):
            logging.debug('creating directory {}'.format(temp_dir))
            os.makedirs(temp_dir)
        if os.path.exists(temp_file):
            filelist = [f for f in os.listdir(temp_dir) if f.endswith(".txt")]
            logging.info('temp files exist and are removed.')
            for file in filelist:
                os.remove(os.path.join(temp_dir, file))
        with open('data/input/' + self._filename, 'r', encoding="utf8") as input_file:
            # Lese die Zeilen in eine Liste.
            lines = input_file.readlines()
            for index, line in enumerate(lines):
                if not re.search(r'^[0-9]{9}', line):
                    continue
                with open(temp_file, 'a+', encoding="utf8") as output_file:
                    output_file.writelines(line)
                    output_file.close()

    # wendet nacheinander alle LineCheckers einer Filter-Kette an.
    def filter(self):
        """
        applies the list of filters to the provided text file
        :return:
        """
        for index, line_checker in enumerate(self._line_checkers):
            logging.info('applying filter number {}: {}'.format(index, line_checker.method_name))
            self.apply_line_checker(index, line_checker)

    # hängt den aktuellen Eintrag an die Ausgabedatei an.
    def append_to_file(self, entry, step):
        """
        writes one entry to the output file
        :param entry: the entry to be written
        :param step: the sequence number of the step
        """
        output_file = 'data/temp/{}/step_{}.txt'.format(self._project, step+1)
        # Die Datei im "Anhängen"-Modus öffnen und die einzelnen Zeilen des Eintrags der Datei anhängen. Dann die Datei
        # schließen.
        with open(output_file, 'a+', encoding="utf8") as output_file:
            output_file.writelines(entry)
            output_file.close()

    # Fügt je nach append_type und Ergbnis der Prüfung den Eintrag an die Ausgabe-Datei an.
    def append_entry_if_necessary(self, entry, is_condition_fulfilled, append_type, step):
        """
        appends the entry (= list of lines) to the output file, depending on the append_type ('append' or 'remove') and
        the result of the LineChecker check.
        :param entry: the list of lines building one entry
        :param is_condition_fulfilled: the result of the LineChecker check
        :param append_type: whether appending or removing entries that fit the given condition
        :param step: the sequence number of the step
        """
        if is_condition_fulfilled and append_type == 'append':
            self.append_to_file(entry, step)
            return 1
        elif not is_condition_fulfilled and append_type == 'remove':
            self.append_to_file(entry, step)
            return 1
        return 0

    def apply_line_checker(self, step, line_checker):
        """
        does the magic and prepares the output lists from an input
        :param step: the sequence number of the step
        :param line_checker: the line checker to be applied
        """
        input_filename = 'data/temp/{}/step_{}.txt'.format(self._project, step)
        # Öffnet die Eingabedatei im Lesen-Modus.
        with open(input_filename, 'r', encoding="utf8") as input_file:

            # Lese die Zeilen in eine Liste.
            lines = input_file.readlines()

            # Erste ID als Startpunkt für den Vergleich einlesen .
            entry_id = lines[0][0:9]

            # im debug prüfen, ob die Grenzen für die Feldwerte und -inhalte korrekt gesetzt sind
            logging.debug(entry_id)
            logging.debug(line_checker.get_field(lines[0]))
            logging.debug(line_checker.get_value(lines[0]))

            # Liste an zu schreibenden Zeilen pro Eintrag vorbereiten.
            entry = []

            # Die Bedingugung als Falsch intialisieren.
            is_condition_fulfilled = False

            # Den Counter für Treffer auf null setzen.
            number_appended_entries = 0

            # Die Anzahl der Einträge auf null setzen.
            total_number_entries = 0

            # alle Zeilen durchgehen.
            for index, line in enumerate(lines):
                if not re.search(r'^[0-9]{9}', line):
                    continue
                # Falls die ID mit der vorherigen Zeile übereinstimmt, gehört sie zum selben Eintrag.
                if entry_id == line[0:9]:

                    # Die Zeile wird der Eintragsliste hinzugefügt.
                    entry.append(line)

                    # Die vorgegebene Bedingung wird durch den LineChecker geprüft. Falls die Bedingung wahr ist,
                    # wird dies in der Variablen 'is_condition_fulfilled' gespeichert.
                    if line_checker.check(line):
                        is_condition_fulfilled = True
                else:
                    # Falls die ID nicht mit der voherigen Zeile übereinstimmt, beginnt ein neuer Eintrag.
                    # Je nach Bedingungen des append_type ('append' oder 'remove') und dem Ergebnis des LineCheckers
                    # wird der Eintrag der Ausgabedatei angehängt.
                    number_appended_entries += self.append_entry_if_necessary(entry,
                                                                              is_condition_fulfilled,
                                                                              line_checker.mode,
                                                                              step)

                    # Die Gesamtzahl der Einträge wird um eins erhöht.
                    total_number_entries += 1

                    # Die Variable 'is_condition_fulfilled' wird wieder auf False zurückgesetzt.
                    is_condition_fulfilled = False

                    # Ein neue Liste an Zeilen für den neuen Eintrag wird mit der aktuellen Liste als erstem Eintrag
                    # gebildet.
                    entry = [line]

                    # Die neue ID festhalten.
                    entry_id = line[0:9]

                    # Auch diese erste Zeile durch den LineChecker prüfen und ggf. die Variable 'is_condition_fulfilled'
                    #  auf 'wahr' setzen.
                    if line_checker.check(line):
                        is_condition_fulfilled = True

            # Auch den letzten Eintrag der Ausgabedatei anhängen, wenn die Bedingugnen erfüllt sind.
            number_appended_entries += self.append_entry_if_necessary(entry,
                                                                      is_condition_fulfilled,
                                                                      line_checker.mode,
                                                                      step)

            # Die Gesamtzahl der Einträge wird um eins erhöht.
            total_number_entries += 1

            # Die Anzahl der Treffer und Fehler auf der Kommandozeile ausgeben
            logging.info('{} of {} matched the criteria "{}" in step {}'.format(number_appended_entries,
                                                                                total_number_entries,
                                                                                line_checker.method_name,
                                                                                step))
            # Die Inputdatei schließen.
            input_file.close()

    def generate_p2e_file(self):
        """
        takes the last temporary file and creates a p2e file of the given type
        """
        # Das Basisverzeichnis ist data/output relativ zum Verzeichnis dieser Datei.
        base_directory = output_dir.format(self._project)
        if not os.path.exists(base_directory):
            logging.info('creating output directory ' + base_directory)
            os.mkdir(base_directory)
        # Der Name der Ausgabedatei
        output_filename = base_directory + '/p2e_' + self._project + '.txt'
        input_filename = 'data/temp/{}/step_{}.txt'.format(self._project, len(self._line_checkers))
        # Wenn die Datei bereits exisitiert, wird sie gelöscht und eine entsprechende Meldung ausgegeben.
        if os.path.exists(output_filename):
            logging.info('output file exists.')
            os.remove(output_filename)
        # Die Datei im "Anhängen"-Modus öffnen und die einzelnen Zeilen des Eintrags der Datei anhängen. Dann die Datei
        # schließen.
        with open(input_filename, 'r', encoding="utf8") as input_file:
            # Lese die Zeilen in eine Liste.
            lines = input_file.readlines()
            sys_old = ''
            for index, line in enumerate(lines):
                sys_new = line[0:9]
                if sys_new == sys_old:
                    continue
                else:
                    with open(output_filename, 'a+', encoding="utf8") as output_file:
                        output_file.writelines('EDU01' + sys_new + ',' + self._record_type + '\n')
                        output_file.close()
                    sys_old = sys_new

    def generate_field_value_list(self, field, short, format=''):
        """
        creates a list of field values from the refined records
        :param field: the field to be extracted
        :param short: whether to use only the short field (three characters) or the long field (four characters)
        """
        if short:
            line_checker = LineChecker(method_name='is_short_field', field=field, format=format)
        else:
            line_checker = LineChecker(method_name='is_field', field=field, format=format)
        # Das Basisverzeichnis ist data/output relativ zum Verzeichnis dieser Datei.
        base_directory = output_dir.format(self._project)
        if not os.path.exists(base_directory):
            os.mkdir(base_directory)
        # Der Name der Ausgabedatei
        output_filename = '{}/field_{}_{}_list.txt'.format(base_directory, field, self._project)
        input_filename = 'data/temp/{}/step_{}.txt'.format(self._project, len(self._line_checkers)-1)
        # Wenn die Datei bereits exisitiert, wird sie gelöscht und eine entsprechende Meldung ausgegeben.
        if os.path.exists(output_filename):
            logging.info('output file exists.')
            os.remove(output_filename)
        # Die Datei im "Anhängen"-Modus öffnen und die einzelnen Zeilen des Eintrags der Datei anhängen. Dann die Datei
        # schließen.
        with open(input_filename, 'r', encoding="utf8") as input_file:
            # Lese die Zeilen in eine Liste.
            lines = input_file.readlines()
            for index, line in enumerate(lines):
                print(line_checker.check(line))
                if line_checker.check(line):
                    with open(output_filename, 'a+', encoding="utf8") as output_file:
                        output_file.writelines(line_checker.get_value(line) + '\n')
                        output_file.close()

    def add_line_checker(self, line_checker):
        """
        adds an additional line checker to the end of the list of line checkers
        :param line_checker: the line checker to be added
        """
        self._line_checkers.append(line_checker)

    def split_urls(self):
        """
        checks whether a given field contains several subfields (e.g. urls) exists and splits it into multiple fields
        containing only one subfield
        """
        # Verzeichnis, wo die eingabedatei liegt
        base_directory = output_dir.format(self._project)
        line_checker = self._line_checkers[0]
        if not os.path.exists(base_directory):
            os.mkdir(base_directory)
        # Der Name der Ausgabedatei
        output_filename = '{}/{}'.format(base_directory, 'out_' + self._filename)
        input_filename = 'data/temp/{}/step_{}.txt'.format(self._project, len(self._line_checkers)-1)
        # Wenn die Datei bereits exisitiert, wird sie gelöscht und eine entsprechende Meldung ausgegeben.
        if os.path.exists(output_filename):
            logging.info('output file exists.')
            os.remove(output_filename)
        # Die Datei im "Anhängen"-Modus öffnen und die einzelnen Zeilen des Eintrags der Datei anhängen. Dann die Datei
        # schließen.
        with open(input_filename, 'r', encoding="utf8") as input_file:
            with open(output_filename, 'a+', encoding="utf8") as output_file:
                # Lese die Zeilen in eine Liste.
                lines = input_file.readlines()
                for index, line in enumerate(lines):
                    if line_checker.check(line):
                        urls = line_checker.get_value(line).split('$$u')
                        for url in urls:
                            if url.endswith('ean='):
                                continue
                            if url == '':
                                continue
                            output_file.writelines('{}$$u{}\n'.format(line[0:line_checker.format.value_start], url))
                    else:
                        output_file.writelines(line)
                output_file.close()
            input_file.close()

    def remove_field(self, field_list):
        base_directory = output_dir.format(self._project)
        line_checker = self._line_checkers[0]
        if not os.path.exists(base_directory):
            os.mkdir(base_directory)
        # Der Name der Ausgabedatei
        output_filename = '{}/{}'.format(base_directory, 'out_' + self._filename)
        input_filename = 'data/temp/{}/step_{}.txt'.format(self._project, len(self._line_checkers))
        # Wenn die Datei bereits exisitiert, wird sie gelöscht und eine entsprechende Meldung ausgegeben.
        if os.path.exists(output_filename):
            logging.info('output file exists.')
            os.remove(output_filename)
        # Die Datei im "Anhängen"-Modus öffnen und die einzelnen Zeilen des Eintrags der Datei anhängen. Dann die Datei
        # schließen.
        with open(input_filename, 'r', encoding="utf8") as input_file:
            with open(output_filename, 'a+', encoding="utf8") as output_file:
                # Lese die Zeilen in eine Liste.
                lines = input_file.readlines()
                for index, line in enumerate(lines):
                    if line_checker.get_field(line) in field_list:
                        continue
                    else:
                        output_file.writelines(line)
                output_file.close()
            input_file.close()


    def test_field_values(self):
        """
        just a test casse to check the validity of the applied format. Prints the identifier, the field and the value
        to the log file.
        """
        for index, line_checker in enumerate(self._line_checkers):
            logging.info('applying filter number {}: {}'.format(index, line_checker.method_name))
            self.apply_line_checker(index, line_checker)
            input_filename = 'data/temp/{}/step_{}.txt'.format(self._project, 0)
            # Öffnet die Eingabedatei im Lesen-Modus.
            with open(input_filename, 'r', encoding="utf8") as input_file:
                # Lese die Zeilen in eine Liste.
                lines = input_file.readlines()
                line_checker.test_field_values(lines[4])
