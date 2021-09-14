import re

from service import coredata_reader_service


def transform_coredata(filename, campus):
    coredata_table = coredata_reader_service.read_table(filename)
    new_table = map_fields(coredata_table, campus)
    coredata_reader_service.write_coredata_table(new_table, campus)


def initalize_row():
    new_row = {}
    new_row['collection'] = ''
    new_row['mediaType'] = ''
    new_row['shelfmark'] = ''
    new_row['minting'] = ''
    new_row['color'] = ''
    new_row['colorMinting'] = ''
    new_row['binding'] = 'K'
    new_row['cover'] = 'GW'
    new_row['positionYear'] = ''
    new_row['positionVolume'] = ''
    new_row['positionPart'] = ''
    new_row['positionDescription'] = ''
    new_row['comment'] = ''
    new_row['bindingsFollow'] = ''
    new_row['vendorAccount'] = ''
    new_row['internalNote'] = ''
    new_row['bubiNote'] = ''
    new_row['active'] = True
    new_row['fund'] = ''
    new_row['coverBack'] = False
    new_row['withoutRemoval'] = False
    return new_row


def map_fields(coredata_table, campus):
    new_rows = []
    pattern = re.compile("\\d\\d[A-Z]\\d+.*")
    pattern_correct = re.compile("\\d\\d [A-Z] \\d+.*")
    for index, row in coredata_table.iterrows():
        new_row = initalize_row()
        if campus == 'D':
            if row['Standort'].startswith('D') or row['Standort'].startswith('D'):
                new_row['collection'] = row['Standort']
            else:
                new_row['collection'] = campus + row['Standort']

            shelfmark= row['Signatur']
            if pattern.match(shelfmark):
                shelfmark = shelfmark[:2] + ' ' + shelfmark[2] + ' ' + shelfmark[3:]
            if pattern_correct.match(shelfmark):
                new_row['mediaType'] = 'JOURNAL'
            else:
                new_row['mediaType'] = 'SERIES'
            new_row['shelfmark'] = shelfmark
            new_row['minting'] = row['Prägung']
            try:
                new_row['color'] = row['Farbe'].split('/')[0]
            except AttributeError:
                print(shelfmark + ': no color given')
            try:
                new_row['colorMinting'] = row['Farbe'].split('/')[1]
            except AttributeError:
                print(shelfmark + ': no color for minting given')
            except IndexError:
                print(shelfmark + ': no color for minting given')
            try:
                new_row['binding'] = row['Bindung'].upper()
            except AttributeError:
                print(shelfmark + ': no binding given')
            new_row['positionYear'] = row['Jahr']
            new_row['positionVolume'] = row['Band']
            new_row['positionPart'] = row['Teil']
            new_row['positionDescription'] = row['Teilangabe']
            new_row['comment'] = row['Bemerkungen Alt']
            try:
                new_row['bindingsFollow'] = row['Bindefolge'].replace(' ', '')
            except AttributeError:
                print(shelfmark + ': no bindungsfolge given')
            try:
                bubi = row['Buchbinder'].strip()
                if bubi == 'Rothe':
                    new_row['vendorAccount'] = 'B-1118'
                elif bubi == 'Blasberg':
                    new_row['vendorAccount'] = 'B-1114'
                elif bubi == 'Knüfken':
                    new_row['vendorAccount'] = 'B-1115'
                else:
                    print(shelfmark + ': other vendor given')
            except AttributeError:
                print(shelfmark + ': no vendor given')
            new_row['internalNote'] = row['Bemerkungen Intern']
            new_row['bubiNote'] = row['Bemerkungen Extern']
            new_row['active'] = row['FF']
            if new_row['mediaType'] == 'JOURNAL':
                new_row['fund'] = '55510-0-1100'
            else:
                new_row['fund'] = '55510-0-1200'
        elif campus == 'E':

            # anhand der Signatur Materialtyp definieren
            shelfmark= row['signatur']
            if pattern.match(shelfmark):
                shelfmark = shelfmark[:2] + ' ' + shelfmark[2] + ' ' + shelfmark[3:]
            if pattern_correct.match(shelfmark):
                new_row['mediaType'] = 'JOURNAL'
            else:
                new_row['mediaType'] = 'SERIES'

            # Signatur speichern
            new_row['shelfmark'] = shelfmark

            # Prägung auslesen
            new_row['minting'] = row['Titel']

            # Farbinformationen auslesen
            try:
                new_row['color'] = row['farbe'].split(',')[0]

            except AttributeError:
                print(shelfmark + ': no color given')
            try:
                color = row['farbe'].split(', ')[1].strip()
                print(color)
                if 'weiss' in color or 'weis' in color or 'weiß' in color:
                    new_row['colorMinting'] = 'w'
                elif 'schwarz' in color:
                    new_row['colorMinting'] = 'b'
                elif 'gold' in color:
                    new_row['colorMinting'] = 'g'
            except AttributeError:
                print(shelfmark + ': no color for minting given')
            except IndexError:
                print(shelfmark + ': no color for minting given')

            # Bandangabe auslesen
            try:
                # an den Zeilenumbrüchen zerlegen:
                list_of_lines = row['Bandangabe'].split('_x000D_\n')
                comment = ''
                for line in list_of_lines:
                    if '///' in line:
                        new_row['part_description'] = line
                        for part in line.split('/'):
                            if part.strip() == '':
                                continue
                            if 'H.' in part or 'S.' in part:
                                new_row['positionPart'] = part.strip()
                                continue
                            match = re.match(r'.*([19,20][0-9]{2})', part)
                            if match is not None:
                                new_row['positionYear'] = part.strip()
                            else:
                                new_row['positionVolume'] = part.strip()
                    elif line.strip() == '':
                        continue
                    else:
                        comment += line + ' '
                        if 'Rücken überziehen' in line or 'Rü. überz' in line:
                            new_row['coverBack'] = True

                        if 'Mit vord. Umschl. und Heftreg. binden' in line:
                            new_row['bindingsFollow'] = 'T+R=00'

                        if 'Mit vord. Umschl. binden' in line:
                            new_row['bindingsFollow'] = 'T=00'
                        # Bindungsart auslesen
                        if 'Fadenheftung' in line:
                            new_row['binding'] = 'F'
                            # binding_comment = binding_comment.replace('Fadenheftung', '')

                        bindings_follow = ''
                        if 'Ohne Entnahme' in line:
                            new_row['withoutRemoval'] = True
                            # binding_comment = binding_comment.replace('Ohne Entnahme', '')
                        if 'T+R=00' in line:
                            new_row['bindingsFollow'] = 'T+R=00'
                            # binding_comment = binding_comment.replace('T+R=00', '')
                        elif 'T=00' in line:
                            new_row['bindingsFollow'] = 'T=00'
                            # binding_comment = binding_comment.replace('T=00', '')
                        elif 'R=00' in line:
                            new_row['bindingsFollow'] = 'R=00'
                            # binding_comment = binding_comment.replace('R=00', '')
                        new_row['bubiNote'] = comment
            except AttributeError:
                print(shelfmark + ': no bandangabe given')
            except TypeError:
                print(shelfmark + ': no bandangabe is NaN')

            # Buchbinder festlegen
            try:
                bubi = row['bubi']
                if bubi == 2:
                    new_row['vendorAccount'] = 'B-1117'
                elif bubi == 3:
                    new_row['vendorAccount'] = 'B-1115'
                elif bubi == 4:
                    new_row['vendorAccount'] = 'B-1118'
                elif bubi == 6:
                    new_row['vendorAccount'] = 'B-1114'
            except AttributeError:
                print(shelfmark + ': no vendor given')
            new_row['active'] = not row['loesch']
            if new_row['mediaType'] == 'JOURNAL':
                new_row['fund'] = '55510-0-1100'
            else:
                new_row['fund'] = '55510-0-1200'
        new_rows.append(new_row)
    return new_rows
