import pandas as pd
import pathlib

DATA_FOLDER_PATH = pathlib.Path('../../data/')
hesa_data_path = DATA_FOLDER_PATH / 'raw/uk_hesa'
output_data_path = DATA_FOLDER_PATH / 'unstable/diversity_uk_cameron'

tablemap_2010_17 = {'Table_2a' : 'academic_',
            'Table_2b' : 'nonacademic_',
            'Table_2c' : 'academicatypical_'}

tablemap_2004_9 = {
                   'Table_23' : 'academic_',
                   'Table_23a' : 'academic_',
                   'Table_23b' : 'academicatypical_'}

tablemap_2002_3 = {
                   'Table 16' : 'academic_'
                    }

def process_year(folder, filenames, year, tablemap):
    year_dataframes = []
    cursor = 0
    for sheet in tablemap.keys():
        if len(filenames) == 1:
            file = filenames[0]
        elif len(filenames) > 1:
            file = filenames[cursor]
            cursor += 1
        print('Reading file:', file, sheet)
        df = pd.read_excel(file, sheet_name=sheet, header=9)

        map = {}
        for colname in list(df):
            map[colname] = colname.replace('\n', '')
        for i, k in enumerate(list(df)):
            if k.startswith('Unknown'):
                map[k] = 'unknown.'+str(i)
        df.rename(columns=map, inplace=True)
        map={}
        
        for colname in list(df):
            map[colname] = 'diversity_uk_hesa_' + tablemap[sheet] + colname.lower().replace('\n', '').replace(' ', '_')
        for colname in ['INSTID', 'UKPRN', 'Region of institution', 'Region of HE provider',
                        'Institution', 'HE provider', 'Unnamed: 3']:
            map[colname] = 'diversity_uk_hesa_' + colname.lower().replace(' ', '_')
        map['Unnamed: 3'] = 'diversity_uk_hesa_institution'
        df.rename(columns=map, inplace=True)
        map={}
        
        map.update({'diversity_uk_hesa_region_of_he_provider': 'diversity_uk_hesa_region_of_institution',
                        'diversity_uk_hesa_unnamed:_3' : 'diversity_uk_hesa_institution',
                        'diversity_uk_hesa_he_provider' : 'diversity_uk_hesa_institution',
                        'diversity_uk_hesa_academic_unknown.16' : 'diversity_uk_hesa_academic_age_unknown',
                        'diversity_uk_hesa_academic_unknown.22' : 'diversity_uk_hesa_academic_disability_unknown',
                        'diversity_uk_hesa_academic_unknown.27' : 'diversity_uk_hesa_academic_ethnicity_unknown',
                        'diversity_uk_hesa_academic_unnamed:_28' : 'diversity_uk_hesa_academic_total',
                        'diversity_uk_hesa_nonacademic_unknown.16' : 'diversity_uk_hesa_nonacademic_age_unknown',
                        'diversity_uk_hesa_nonacademic_unknown.22' : 'diversity_uk_hesa_nonacademic_disability_unknown',
                        'diversity_uk_hesa_nonacademic_unknown.27' : 'diversity_uk_hesa_nonacademic_ethnicity_unknown',
                        'diversity_uk_hesa_nonacademic_unnamed:_28' : 'diversity_uk_hesa_nonacademic_total',
                        'diversity_uk_hesa_academicatypical_unknown.16' : 'diversity_uk_hesa_academicatypical_age_unknown',
                        'diversity_uk_hesa_academicatypical_unknown.22' : 'diversity_uk_hesa_academicatypical_disability_unknown',
                        'diversity_uk_hesa_academicatypical_unknown.27' : 'diversity_uk_hesa_academicatypical_ethnicity_unknown',
                        'diversity_uk_hesa_academicatypical_unnamed:_28' : 'diversity_uk_hesa_academicatypical_total'})
        #print(list(df))
        df.rename(columns=map, inplace=True)
        df.dropna(inplace=True)
        df.set_index('diversity_uk_hesa_instid')
        year_dataframes.append(df)
    
    year_df = pd.concat(year_dataframes, axis=1, join='inner')
    year_df.drop(columns=['diversity_uk_hesa_instid'])
    year_df['year'] = year
    return year_df




folders_2009_12 = list(hesa_data_path.glob('201*-1*'))
folders_2009_12.extend(list(hesa_data_path.glob('HESA_Staff_2009*')))

for folder in folders_2009_12:
    print('\n',folder)
    filelist = list(folder.glob('**/[Ss]taff_*_[Tt]able_2[abc].xls'))
    filelist.sort()
    foldername = str(folder)
    year = 2000 + int(foldername[len(foldername)-2:len(foldername)])
    year_df = process_year(folder, filelist, year, tablemap_2010_17)
    filename = 'hesa_'+ str(year) +'.csv'
    year_df.to_csv(output_data_path / filename, index=False)

for folder in hesa_data_path.glob('200?-0[5-9]'):
    print('\n', folder)
    filelist = list(folder.glob('**/Table_23[ab].xls'))
    filelist.sort()
    foldername = str(folder)
    year = 2000 + int(foldername[len(foldername)-2:len(foldername)])

    sheet = 'Table_23a'
    file = filelist[0]
    print('Reading file:', file, sheet)
    df_acad = pd.read_excel(file, sheet_name=sheet, skiprows=5, na_values='..')
    map = {
            'Unnamed: 0' : 'diversity_uk_hesa_institution', 
            'Unnamed: 1' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_average_age' , 
            'Unnamed: 2' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_%_<_35', 
            'Unnamed: 3' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_%_>_55', 
            'Unnamed: 4' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_female', 
            'Unnamed: 5' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_male', 
            'Unnamed: 6' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_total', 
            'Unnamed: 7' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_average_age' , 
            'Unnamed: 8' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_%_<_35', 
            'Unnamed: 9' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_%_>_55', 
            'Unnamed: 10' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_female', 
            'Unnamed: 11' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_male', 
            'Unnamed: 12' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_total', 
            'Unnamed: 13' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'total'
    }
    df_acad.rename(columns=map, inplace=True)
    df_acad.dropna(inplace=True, thresh=6)

    sheet = 'Table_23b'
    file = filelist[1]
    print('Reading file:', file, sheet)
    df_atypical = pd.read_excel(file, sheet_name=sheet, skiprows=4, na_values='..')
    map = {
            'Unnamed: 0' : 'diversity_uk_hesa_institution', 
            'Unnamed: 1' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'average_age' , 
            'Unnamed: 2' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + '%_<_35', 
            'Unnamed: 3' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + '%_>_55', 
            'Unnamed: 4' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'female', 
            'Unnamed: 5' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'male', 
            'Unnamed: 6' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'gender_unknown', 
            'Unnamed: 7' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'total'
    }
    df_atypical.rename(columns=map, inplace=True)
    df_atypical.dropna(inplace=True, thresh=4)

    year_df = pd.concat([df_acad, df_atypical], axis=1)
    filename = 'hesa_'+ str(year) +'.csv'
    year_df['year'] = year
    year_df.to_csv(output_data_path / filename, index=False)

for folder in hesa_data_path.glob('2003-04'):
    print('\n', folder)
    filelist = list(folder.glob('**/table_23.xls'))
    filelist.sort()
    foldername = str(folder)
    year = 2000 + int(foldername[len(foldername)-2:len(foldername)])

    sheet = 'Table_23'
    file = filelist[0]
    print('Reading file:', file, sheet)
    df_acad = pd.read_excel(file, sheet_name=sheet, skiprows=6, na_values='..')
    map = {
            'Unnamed: 0' : 'diversity_uk_hesa_institution', 
            'Unnamed: 1' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_average_age' , 
            'Unnamed: 2' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_%_<_35', 
            'Unnamed: 3' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_%_>_55', 
            'Unnamed: 4' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_female', 
            'Unnamed: 5' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_male', 
            'Unnamed: 6' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'full_time_total', 
            'Unnamed: 7' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_average_age' , 
            'Unnamed: 8' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_%_<_35', 
            'Unnamed: 9' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_%_>_55', 
            'Unnamed: 10' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_female', 
            'Unnamed: 11' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_male', 
            'Unnamed: 12' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'part_time_total', 
            'Unnamed: 13' : 'diversity_uk_hesa_' + tablemap_2004_9[sheet] + 'total'
    }
    df_acad.rename(columns=map, inplace=True)
    df_acad.dropna(inplace=True, thresh=6)
    filename = 'hesa_'+ str(year) +'.csv'
    df_acad['year'] = year
    df_acad.to_csv(output_data_path / filename, index=False)

for folder in hesa_data_path.glob('2002-03'):
    print('\n', folder)
    filelist = list(folder.glob('Tables/Staff/table_16.xls'))
    filelist.sort()
    foldername = str(folder)
    year = 2000 + int(foldername[len(foldername)-2:len(foldername)])

    sheet = 'Table 16'
    file = filelist[0]
    print('Reading file:', file, sheet)
    df_acad = pd.read_excel(file, sheet_name=sheet, skiprows=6, na_values='..')
    map = {
            'Unnamed: 0' : 'discard',
            'Unnamed: 1' : 'diversity_uk_hesa_institution',
            'Unnamed: 2' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'full_time_average_age' , 
            'Unnamed: 3' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'full_time_%_<_35', 
            'Unnamed: 4' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'full_time_%_>_55', 
            'Unnamed: 5' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'full_time_female', 
            'Unnamed: 6' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'full_time_male', 
            'Unnamed: 7' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'full_time_total', 
            'Unnamed: 8' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'part_time_average_age' , 
            'Unnamed: 9' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'part_time_%_<_35', 
            'Unnamed: 10' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'part_time_%_>_55', 
            'Unnamed: 11' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'part_time_female', 
            'Unnamed: 12' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'part_time_male', 
            'Unnamed: 13' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'part_time_total', 
            'Unnamed: 14' : 'diversity_uk_hesa_' + tablemap_2002_3[sheet] + 'total'
    }
    df_acad.rename(columns=map, inplace=True)
    df_acad.dropna(inplace=True, thresh=6)
    df_acad.drop('discard', axis=1)
    filename = 'hesa_'+ str(year) +'.csv'
    df_acad['year'] = year
    df_acad.to_csv(output_data_path / filename, index=False)

for folder in hesa_data_path.glob('HESA_Staff_201*'):
    print('\n',folder)
    filelist = list(folder.glob('**/staff_*_all_main_tables.xlsx'))
    foldername = str(folder)
    year = 2000 + int(foldername[len(foldername)-2:len(foldername)])

    year_df = process_year(folder, filelist, year, tablemap_2010_17)
    filename = 'hesa_'+ str(year) +'.csv'
    year_df.to_csv(output_data_path / filename, index=False)