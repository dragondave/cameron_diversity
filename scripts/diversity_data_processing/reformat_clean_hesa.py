from fuzzywuzzy import process
import csv
import json
import copy
import datetime
from pathlib import Path
import re
import datetime

notes_re = re.compile('\(#[0-9]{1,2}\)')

DATA_FOLDER_PATH = Path('../../data/')
hesa_data_path = DATA_FOLDER_PATH / 'raw/uk_hesa'
output_data_path = DATA_FOLDER_PATH / 'unstable/diversity_uk_cameron'
grid_json_path = DATA_FOLDER_PATH /'raw/grid/grid.json'
with open(grid_json_path) as f:
    grid = json.load(f)['institutes']
grid_uk = [uni for uni in grid if (uni.get('addresses') and uni['addresses'][0].get('country') == 'United Kingdom')]
grid_dict = dict([(uni.get('id'), uni) for uni in grid_uk])
grid_names = [institute.get('name').lower() for institute in grid_uk]
pass_list = []

if not Path('grid_lookup.json').is_file():
    with open('grid_lookup.json', 'w') as f:
        json.dump({}, f)
with open('grid_lookup.json', 'r') as f:
    grid_lookup = json.load(f)

def get_grid(name):
    name = name.lower()
    if name in grid_lookup.keys():
        return grid_lookup[name]

    if name in grid_names:
        index = grid_names.index(name)
        return grid_uk[index].get('id')

    elif name.replace('the ', '', 1) in grid_names:
        index = grid_names.index(name.replace('the ', '', 1))
        return grid_uk[index].get('id')
    
    fuzznames = grid_names + [name for name in grid_lookup.keys()]
    fuzzmatch = process.extractBests(name.replace('university', '').replace('the', ''), fuzznames)
    print('\nName to match:', name)
    for i, match in enumerate(fuzzmatch):
        print('  '+str(i)+': '+match[0])
    i = input('Select a match (0-indexed) or give the correct GRID:')
    try:
        i = int(i)
        return grid_uk[grid_names.index(fuzzmatch[i][0])].get('id')
    except ValueError:
        return i

def get_grid_elements(grid_id):
    return {'grid' : grid_id,
            'uni_name' : grid_dict[grid_id]['name']}

def clean_notes(string):
    string = string.lstrip()
    string = string.rstrip('*‡† ')
    string = re.split(r'\(#[0-9]{1,2}\)', string)[0]
    return string

print(output_data_path)
for file in output_data_path.glob('hesa_20*.csv'):
    pass # I don't know what the hell is going on here, seems like some weird interaction between
        # pathlib and google drive maybe?

agg_data = []
header_items = ['grid', 'year', 'uni_name', 'country', 'region', 'subregion',
                'data_category', 'data_name', 'data_display_name', 'data_type', 'data_value',
                'data_source', 'data_last_processed', 'data_note']

default_items = {'country' : 'United Kingdom',
                 'region' : 'Europe',
                 'subregion' : 'Northern Europe',
                 'data_category' : 'diversity_uk',
                 'data_source' : 'UK HESA',
                 'data_note' : 'Processed via scripts process_hesa.py and clean_reformat_hesa.py'
}

for file in output_data_path.glob('hesa_20*.csv'):
    with open(file) as f:
        print('Reading file:', file)
        reader = csv.DictReader(f)
        data = [line for line in reader]
    
    for uni in data:
        if uni['diversity_uk_hesa_institution'].startswith('Total'):
            continue
        uni_name = clean_notes(uni['diversity_uk_hesa_institution'])
        if uni_name in pass_list:
            continue
        grid = get_grid(uni_name)
        if grid not in grid_dict.keys():
            pass_list.append(uni_name)
            continue
        year = uni['year']
        if uni_name.lower() not in grid_lookup.keys():
            grid_lookup[uni_name.lower()] = grid
            with open('grid_lookup.json', 'w') as f:
                json.dump(grid_lookup, f)
        
        for k in uni.keys():
            if k in ['diversity_uk_hesa_institution', 'diversity_uk_hesa_instid','diversity_uk_hesa_ukprn',
                     'diversity_uk_hesa_region_of_institution', 'year']:
                continue
            data_dict = copy.copy(default_items)
            data_dict.update(get_grid_elements(grid))
            data_dict.update({'uni_name' : uni_name,
                              'year' : year,
                              'data_name' : clean_notes(k),
                              'data_display_name' : (' ').join(clean_notes(k).split('_')).title(),
                              'data_value' : uni[k],
                              'data_last_processed' : datetime.datetime.today().isoformat()
                              })
            agg_data.append(data_dict)

with open('test.csv', 'w') as f:
    writer  = csv.DictWriter(f, fieldnames=header_items)
    writer.writeheader()
    writer.writerows(agg_data)

        

        


        
        