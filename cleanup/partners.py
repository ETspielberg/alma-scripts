import json
import logging
import os
import time

import requests

from service.list_reader_service import load_identifier_list_of_type


alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def update_partners(project):
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for partner in partners:
        time.sleep(0.5)
        logging.info('processing partner {}'.format(partner))

        # Die URL für die API zusammensetzen
        url = '{}partners/{}?apikey={}'.format(alma_api_base_url, partner, api_key)

        # Die GET-Abfrage ausführen
        response = requests.get(url=url, headers={'Accept': 'application/json'})

        # Die Kodierung der Antwort auf UTF-8 festlegen
        response.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if response.status_code == 200:
            logging.debug('got partner {} : \n {}'.format(partner, response.text))
            # Die Antwort als Json auslesen, den Wert aus dem Feld 'total_record_count' auslesen und prüfen, ob dieser
            # 0 ist (= keine Rechnungen am Lieferanten)
            partner_json = response.json()

            # Dieser boolean entscheidet, ob der Code des Partners geändert wurde.
            changed_code = False
            try:
                # die ISO-Details auslesen
                iso_details = partner_json['partner_details']['profile_details']['iso_details']
                # den Server setzen
                iso_details['ill_server'] = 'zfl2-test.hbz-nrw.de'
                # den Port setzen
                iso_details['ill_port'] = '33242'
                # das Resending overdue message interval setzen
                iso_details['resending_overdue_message_interval'] = 21
                # das iso-Symbol auslesen
                iso_symbol = iso_details['iso_symbol']
                # falls noch kein DE- im Symbol ist, dieses ergänzen
                if 'DE-' in iso_symbol:
                    iso_symbol = iso_symbol.replace('DE-', '')
                    iso_symbol = iso_symbol.capitalize()
                    iso_symbol = 'DE-' + iso_symbol
                else:
                    iso_symbol = 'DE-' + partner.capitalize()
                # ggf. die Schrägstriche entfernen
                iso_symbol = iso_symbol.replace("/", "-")
                # den code entsprechend ändern und den boolean entsprechend setzen
                #if (partner_json['partner_details']['code'] == iso_symbol.upper()):
                changed_code = True
                logging.debug('changing code from {} to {}'.format(iso_symbol.upper(), iso_symbol))
                partner_json['partner_details']['code'] = iso_symbol
                iso_details['iso_symbol'] = iso_symbol
                logging.debug('set iso symbol for partner {} to {}'.format(partner, iso_symbol))
                changed_code = True
                name = partner_json['partner_details']['name']
                if '(DE-' not in name:
                    name = name.split(' (')[0] + ' (' + iso_symbol + ')'
                else:
                    name = name.replace(iso_symbol.upper(), iso_symbol)
                partner_json['partner_details']['name'] = name
            except KeyError:
                logging.error('no iso details for partner {}'.format(partner))

            # generelle Partner details setzen
            partner_details = partner_json['partner_details']
            partner_details['borrowing_supported'] = True
            partner_details['lending_supported'] = True
            partner_details['borrowing_workflow'] = 'DEFAULT_BOR_WF'
            partner_details['lending_workflow'] = 'DEFAULT_LENDING_WF'

            # Adressen durchgehen, alle bis auf die preferred löschen
            for address in partner_json['contact_info']['address']:
                logging.debug('is preferred address ? {}'.format(address['preferred']))
                if not address['preferred']:
                    logging.debug('removing non-preferred address')
                    partner_json['contact_info']['address'].remove(address)

            # E-Mails durchgehen, alle bis auf die preferred löschen, die preferred zusätzlich in den iso-details eintragen
            for email in partner_json['contact_info']['email']:
                logging.debug('is preferred email ? {}'.format(email['preferred']))
                if email['preferred']:
                    logging.debug('setting iso-email address to preferred address')
                    partner_json['partner_details']['profile_details']['iso_details']['email_address'] = email['email_address']
                else:
                    logging.debug('removing non-preferred email-address')
                    partner_json['contact_info']['email'].remove(email)

            # Update der Adressdaten anhand von Google Maps Ergebnissen
            #google_data = get_google_data(address_block)
            #if google_data is not None:
            #    for address_data in google_data['result']['address_components']:
            #        if 'country' in address_data['types']:
            #            if address_data['short_name'] == 'DE':
            #                partner_json['contact_info']['address'][0]['country'] = {"value": "DEU"}
            #        elif 'locality' in address_data['types']:
            #            partner_json['contact_info']['address'][0]['city'] = address_data['long_name']

            # wenn der Partner code ersetzt wurde, kein Update des Partners durchführen, sondern einen neuen Partner anlegen
            if changed_code:
                # URL für POST (ohne Partner-Code)
                url_new = '{}partners?apikey={}'.format(alma_api_base_url, api_key)
                # Partner als neuen Partner speichern
                logging.debug(json.dumps(partner_json))
                new_partner = requests.post(url=url_new, data=json.dumps(partner_json).encode('utf-8'),
                                              headers={'Content-Type': 'application/json'})
                # nur wenn das Anlegen erfolgreich war, den alten Partner löschen
                if new_partner.status_code == 200 or new_partner.status_code == 204:
                    logging.info('succesfully created partner {}'.format(new_partner.text))
                    requests.delete(url=url)
                    logging.info('deleted old partner')
                else:
                    logging.warn(new_partner.text)

            # wenn der Partner-Code nicht ersetzt wurde, ein einfaches Update durchführen.
            else:
                # Update als PUT-Abfrage ausführen. URL ist die gleiche, Encoding ist wieder utf-8, Inhalt ist JSON
                url = '{}partners/{}?apikey={}'.format(alma_api_base_url, iso_symbol, api_key)
                update = requests.put(url=url, data=json.dumps(partner_json).encode('utf-8'),
                                              headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
                # Die Kodierung der Antwort auf UTF-8 festlegen
                update.encoding = 'utf-8'

                # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
                if update.status_code == 200:
                    logging.info('succesfully updated partner {}'.format(partner))
                else:
                    logging.error('problem updating partner {}:{}'.format(partner, update.text))
        else:
            logging.error(response.text)


def extend_partner_names(project):
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # API-Key aus den Umgebungsvariablen lesen
    # api_key = os.environ['ALMA_SCRIPT_API_KEY']
    api_key = os.environ['ALMA_API_WUP']

    # alle Lieferanten durchgehen
    for partner in partners:
        time.sleep(0.5)

        # Die URL für die API zusammensetzen
        url = '{}partners/{}?apikey={}'.format(alma_api_base_url, partner, api_key)
        try:
            # Die GET-Abfrage ausführen
            response = requests.get(url=url, headers={'Accept': 'application/json'})

            # Die Kodierung der Antwort auf UTF-8 festlegen
            response.encoding = 'utf-8'

            # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
            if response.status_code == 200:

                # Die Antwort als Json auslesen, den Wert aus dem Feld 'total_record_count' auslesen und prüfen, ob dieser
                # 0 ist (= keine Rechnungen am Lieferanten)
                partner_json = response.json()

                try:
                    if partner_json['partner_details']['profile_details']['profile_type'] == 'SLNP':
                        symbol = partner_json['partner_details']['profile_details']['iso_details']['iso_symbol']
                        if 'DE-' in symbol:
                            symbol = symbol.replace('DE-', '')
                        symbol = 'DE-' + symbol.capitalize()
                        partner_json['partner_details']['profile_details']['iso_details']['iso_symbol'] = symbol
                        name = partner_json['partner_details']['name']
                        if not name.startswith('DE-'):
                            partner_json['partner_details']['name'] = '{} ({})'.format(symbol, name)
                        update = requests.put(url=url, data=json.dumps(partner_json).encode('utf-8'),
                                              headers={'Content-Type': 'application/json'})

                        # Die Kodierung der Antwort auf UTF-8 festlegen
                        update.encoding = 'utf-8'

                        # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
                        if update.status_code == 200:
                            logging.info('succesfully updated partner {}'.format(partner))
                        else:
                            logging.error('problem updating partner {}:{}'.format(partner, update.text))
                    else:
                        logging.warning('could not update partner {}: no SLNP'.format(partner))
                        logging.warning(response.text)
                except KeyError:
                    logging.error('no iso details for partner {}'.format(partner))
            else:
                logging.warning(response.text)
        except:
            logging.error('error upon api connection: {}'.format(partner))


def update_partners_resending_due_interval(project):
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for partner in partners:

        # Die URL für die API zusammensetzen
        url = '{}partners/{}?apikey={}'.format(alma_api_base_url, partner, api_key)

        # Die GET-Abfrage ausführen
        response = requests.get(url=url, headers={'Accept': 'application/json'})

        # Die Kodierung der Antwort auf UTF-8 festlegen
        response.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if response.status_code == 200:

            # Die Antwort als Json auslesen, den Wert aus dem Feld 'total_record_count' auslesen und prüfen, ob dieser
            # 0 ist (= keine Rechnungen am Lieferanten)
            partner_json = response.json()

            try:
                iso_details = partner_json['partner_details']['profile_details']['iso_details']
                iso_details['resending_overdue_message_interval'] = 21
            except KeyError:
                logging.error('no iso details for partner {}'.format(partner))
            update = requests.put(url=url, data=json.dumps(partner_json).encode('utf-8'),
                                  headers={'Content-Type': 'application/json'})

            # Die Kodierung der Antwort auf UTF-8 festlegen
            update.encoding = 'utf-8'

            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update.status_code == 200:
                logging.info('succesfully updated partner {}'.format(partner))
            else:
                logging.error('problem updating partner {}:{}'.format(partner, update.text))
        else:
            logging.error(response.text)

def correct_capizalization_in_symbols(project):
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # API-Key aus den Umgebungsvariablen lesen
    api_key = os.environ['ALMA_SCRIPT_API_KEY']

    # alle Lieferanten durchgehen
    for partner in partners:

        # Die URL für die API zusammensetzen
        url = '{}partners/{}?apikey={}'.format(alma_api_base_url, partner, api_key)

        # Die GET-Abfrage ausführen
        response = requests.get(url=url, headers={'Accept': 'application/json'})

        # Die Kodierung der Antwort auf UTF-8 festlegen
        response.encoding = 'utf-8'

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if response.status_code == 200:

            # Die Antwort als Json auslesen, den Wert aus dem Feld 'total_record_count' auslesen und prüfen, ob dieser
            # 0 ist (= keine Rechnungen am Lieferanten)
            partner_json = response.json()

            try:
                symbol = partner_json['partner_details']['profile_details']['iso_details']['iso_symbol']
                symbol = symbol.replace('DE-', '')
                symbol = symbol.capitalize()
                symbol = 'DE-' + symbol
                partner_json['partner_details']['profile_details']['iso_details']['iso_symbol'] = symbol

            except KeyError:
                logging.error('no iso details for partner {}'.format(partner))
            update = requests.put(url=url, data=json.dumps(partner_json).encode('utf-8'),
                                  headers={'Content-Type': 'application/json'})

            # Die Kodierung der Antwort auf UTF-8 festlegen
            update.encoding = 'utf-8'

            # Prüfen, ob Anfrage erfolgreich war und alles in die Log-Datei schreiben
            if update.status_code == 200:
                logging.info('succesfully updated partner {}'.format(partner))
            else:
                logging.error('problem updating partner {}:{}'.format(partner, update.text))
        else:
            logging.error(response.text)
