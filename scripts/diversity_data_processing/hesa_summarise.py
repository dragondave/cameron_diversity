import sys
sys.path.append('./')

from data_processor import IntSummer, Percenter
from pathlib import Path

test = Path('test.csv')

gender = ['female', 'male', 'gender_unknown']
ability = ['known_to_be_disabled', 'no_known_disability', 'disability_unknown']
ethnicity = ['white', 'black', 'asian', 'other_(including_mixed)', 'ethnicity_unknown']

diversity_categories = [gender, ability, ethnicity]

roles = ['academic', 'nonacademic', 'academicatypical', 'academic_full_time', 'academic_part_time']

pc = Percenter(test, target_data_name=None, input_data_names=None)
pc.load()
pc.index_gridyears()


# Percentage of diversity group as a percentage within roles
for category in diversity_categories:
    for role in roles:
        for group in category:
            target_data_name = 'diversity_coki_uk_{}_percent_of_role_{}'.format(role, group)
            input_data_names = {
                                        'numerator' : {'target_data_name' : None,
                                                        'input_data_names' : ['diversity_uk_hesa_{}_{}'.format(role, group)]},
                                        'denominator' : {'target_data_name' : None,
                                                        'input_data_names' : ['diversity_uk_hesa_{}_{}'.format(role, g) for g in category]}
                        }

            pc.target_data_name = target_data_name
            pc.input_data_names = input_data_names
            pc.process(collectsums=False)

# Percentage of academic, including atypical, part-time full-time numbers and percentages within roles
for category in diversity_categories:
    for group in category:
        target_data_name = 'diversity_coki_uk_academic_all_percent_of_academic_{}'.format(group)
        input_data_names = {'numerator' : { 'target_data_name' : 'diversity_coki_uk_academic_all_{}'.format(group),
                                            'input_data_names' : ['diversity_uk_hesa_academic_{}'.format(group),
                                                                  'diversity_uk_hesa_academic_full_time_{}'.format(group),
                                                                  'diversity_uk_hesa_academic_part_time_{}'.format(group), 
                                                                  'diversity_uk_hesa_academicatypical_{}'.format(group)]
                                                                  },
                            'denominator' : {'target_data_name' : 'diversity_coki_uk_academic_all_total',
                                            'input_data_names' : ['diversity_uk_hesa_academic_{}'.format(g) for g in category] + 
                                                                 ['diversity_uk_hesa_academicatypical_{}'.format(g) for g in category] +
                                                                 ['diversity_uk_hesa_academic_full_time_{}'.format(g) for g in category] +
                                                                 ['diversity_uk_hesa_academic_part_time_{}'.format(g) for g in category]
                                                                  }
                            }
    
        pc.target_data_name = target_data_name
        pc.input_data_names = input_data_names
        pc.process(collectsums=True)


pc.save('test_agg.csv')
    