import os


def generate_output():
    with open('data/output/all_records.xml', "w", encoding='utf-8') as output_file:
        output_file.write('<collection>\n')
        for file in os.listdir('Y:\Dezernat 3 - Medienbearbeitung\E-Books\Metadaten\Benjamins\marcxml'):
            with open('Y:\Dezernat 3 - Medienbearbeitung\E-Books\Metadaten\Benjamins\marcxml\{}'.format(file), 'r', encoding='utf-8') as input_file:
                output_file.write(input_file.read())
                input_file.close()
        output_file.write('</collection>\n')
        output_file.close()


if __name__ == '__main__':
    generate_output()
