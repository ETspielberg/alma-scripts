import logging
import os

from service.alma.alma_partner_service import get_partner, update_partner, save_new_partner, delete_partner
from service.list_reader_service import load_identifier_list_of_type
from service.table_reader_service import read_partners_recode_table

alma_api_base_url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/'


def unscrub_emails(project):
    partners = load_identifier_list_of_type(project)
    # alle Lieferanten durchgehen
    for partner in partners:
        sigel = 'DE-' + partner.capitalize()
        logging.info('processing partner {}'.format(sigel))

        # Die URL für die API zusammensetzen
        partner_json = get_partner(sigel)

        # Prüfen, ob die Abfrage erfolgreich war (Status-Code ist dann 200)
        if partner_json is not None:

            # Dieser boolean entscheidet, ob der Code des Partners geändert wurde.
            changed_code = False
            try:
                # die ISO-Details auslesen
                email_address = partner_json['partner_details']['profile_details']['iso_details']['email_address']
                if len(email_address) == 59:
                    logging.warning("email of partner {} os potentially cut")
                if 'SCRUBBED_' in email_address:
                    partner_json['partner_details']['profile_details']['iso_details'][
                        'email_address'] = email_address.replace('SCRUBBED_', '')
                    update_partner(partner_json)
                else:
                    logging.info('email of partner {} does not need to be unscrubbed'.format(sigel))
            except KeyError:
                logging.warning('no email address in iso details in partner ' + sigel)


def update_partners(project):
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # alle Lieferanten durchgehen
    for partner in partners:
        logging.info('processing partner {}'.format(partner))

        partner_json = get_partner(partner)
        if partner_json is not None:
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
                # if (partner_json['partner_details']['code'] == iso_symbol.upper()):
                changed_code = True
                logging.debug('changing code from {} to {}'.format(iso_symbol.upper(), iso_symbol))
                partner_json['partner_details']['code'] = iso_symbol
                iso_details['iso_symbol'] = iso_symbol
                logging.debug('set iso symbol for partner {} to {}'.format(partner, iso_symbol))
                changed_code = True
                name = partner_json['partner_details']['name']
                # if '(DE-' not in name:
                #    name = name.split(' (')[0] + ' (' + iso_symbol + ')'
                # else:
                #    name = name.replace(iso_symbol.upper(), iso_symbol)
                if 'DE-' not in name:
                    name = iso_symbol + ' (' + name.split(' (')[1]
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
                    partner_json['partner_details']['profile_details']['iso_details']['email_address'] = email[
                        'email_address']
                else:
                    logging.debug('removing non-preferred email-address')
                    partner_json['contact_info']['email'].remove(email)

            # Update der Adressdaten anhand von Google Maps Ergebnissen
            # google_data = get_google_data(address_block)
            # if google_data is not None:
            #    for address_data in google_data['result']['address_components']:
            #        if 'country' in address_data['types']:
            #            if address_data['short_name'] == 'DE':
            #                partner_json['contact_info']['address'][0]['country'] = {"value": "DEU"}
            #        elif 'locality' in address_data['types']:
            #            partner_json['contact_info']['address'][0]['city'] = address_data['long_name']

            # wenn der Partner code ersetzt wurde, kein Update des Partners durchführen, sondern einen neuen Partner anlegen
            if changed_code:
                # URL für POST (ohne Partner-Code)
                save_new_partner(partner_json)

            # wenn der Partner-Code nicht ersetzt wurde, ein einfaches Update durchführen.
            else:
                update_partner(partner_json)


def extend_partner_names(project):
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # alle Lieferanten durchgehen
    for partner in partners:

        partner_json = get_partner(partner)

        # Prüfen, ob die Abfrage erfolgreich war
        if partner_json is not None:
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
                    update_partner(partner_json)
                else:
                    logging.warning('could not update partner {}: no SLNP'.format(partner))
            except KeyError:
                logging.error('no iso details for partner {}'.format(partner))


def update_partners_resending_due_interval(project):
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # alle Lieferanten durchgehen
    for partner in partners:

        partner_json = get_partner(partner)
        # Prüfen, ob die Abfrage erfolgreich war
        if partner_json is not None:

            try:
                iso_details = partner_json['partner_details']['profile_details']['iso_details']
                iso_details['resending_overdue_message_interval'] = 21
            except KeyError:
                logging.error('no iso details for partner {}'.format(partner))
            update_partner(partner_json)


def correct_capizalization_in_symbols(project):
    # Datei mit den Lieferanten eines Typs laden
    partners = load_identifier_list_of_type(project)

    # alle Lieferanten durchgehen
    for partner in partners:
        partner_json = get_partner(partner)

        # Prüfen, ob die Abfrage erfolgreich war
        if partner_json < 300:
            try:
                symbol = partner_json['partner_details']['profile_details']['iso_details']['iso_symbol']
                symbol = symbol.replace('DE-', '')
                symbol = symbol.capitalize()
                symbol = 'DE-' + symbol
                partner_json['partner_details']['profile_details']['iso_details']['iso_symbol'] = symbol

            except KeyError:
                logging.error('no iso details for partner {}'.format(partner))
            update_partner(partner)


def recode_partners():
    partners_to_recode = read_partners_recode_table()
    for index, row in partners_to_recode.iterrows():
        partner = get_partner(row['Code alt'])
        try:
            partner['partner_details']['profile_details']['iso_details']['ill_server'] = row['Server']
        except KeyError:
            logging.warning('no ill_server field for partner {}'.format(row['Code alt']))
        partner['partner_details']['code'] = row['Code neu']
        success = save_new_partner(partner)
        if success:
            logging.info('partner to delete: ' + (row['Code alt']))


def delete_partners(project):
    partners = load_identifier_list_of_type(project)
    for partner in partners:
        delete_partner(partner)

