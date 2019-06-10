import csv
import os
import datetime
import copy

DATA_FOLDER_PATH = '../../data/'
filler = 'to_be_added'

folder = os.path.join(DATA_FOLDER_PATH, 'unstable/diversity_au_cameron')
filename = 'au_ucube_gender_data_2001-2018.csv'
with open(os.path.join(folder, filename)) as f:
    reader = csv.DictReader(f)
    entries = [entry for entry in reader]

data_out = []
outputdictfields = ['grid',
                    'year',
                    'uni_name',
                    'groupings',
                    'country',
                    'region',
                    'subregion',
                    'data_category',
                    'data_name',
                    'data_display_name',
                    'data_type',
                    'data_value',
                    'data_source',
                    'data_last_processed',
                    'data_note']

for entry in entries:
    for data_element in ['Women Above Senior Lecturer','Women Senior lecturer (Level C)','Women Lecturer (Level B)',
                         'Women Below lecturer (Level A)','Total women academic','Women non-academic','Total women staff',
                         'All staff above Senior Lecturer','All staff Senior lecturer (Level C)','All staff Lecturer (Level B)',
                         'All staff Below lecturer (Level A)','Total All staff academic','All staff Non-academic',
                         'Total staff', '% Women above senior lecturer','% Women Senior lecturer (Level C)',
                         '% Women Lecturer (Level B)','% Women below lecturer (Level A)','% women academics',
                         '% women non-academic','Total % women staff','Women % all Above Senior Lecturer',
                         'Women % all Senior lecturer (Level C)','Women % all Lecturer (Level B)',
                         'Women % all Below lecturer (Level A)','Women % all staff tacademic',
                         'Women % all Non academic','% Men']:
        d = dict([(field, entry.get(field, filler)) for field in outputdictfields])
        d['data_category'] = 'diversity_au'
        d['data_name'] = 'diversity_au_doe_' + '_'.join(data_element.lower().split(' '))
        d['data_display_name'] = data_element
        data_value = entry[data_element].replace(',','')
        if data_value.isdigit():
            d['data_value'] = int(data_value)
            d['data_type'] = 'int'
        else:
            try:
                d['data_value'] = float(data_value)
                d['data_type'] = 'float'
            except ValueError:
                d['data_value'] = None
                d['data_type'] = 'float'
        d['data_source'] = 'UCube DoE data'
        d['data_last_processed'] = datetime.date.today().isoformat()
        d['data_note'] = 'processed from the UCube reformatted data'

        data_out.append(copy.copy(d))

with open(os.path.join(folder, 'au_gender_diversity_reformat_2000-2018.csv'), 'w', encoding = 'utf-8') as f:
    writer = csv.DictWriter(f, fieldnames = outputdictfields)
    writer.writeheader()
    writer.writerows(data_out)