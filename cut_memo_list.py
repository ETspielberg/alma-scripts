from service.table_reader_service import read_memolist_table, write_memolist_table

if __name__ == '__main__':
    table = read_memolist_table()
    entries = []
    for index, row in table.iterrows():
        old_order_id = ''
        if row['noch notwendig?'].lower().strip() == 'nein':
            continue
        order_id = row['Bestellnummer'].strip()
        if old_order_id == '':
            old_order_id = order_id
        if order_id == '':
            entries.append(row)
        else:
            write_memolist_table(entries, order_id=old_order_id)
            entries = [row]
            old_order_id = order_id

