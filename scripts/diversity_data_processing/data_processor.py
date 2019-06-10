from pathlib import Path
import csv
import datetime
import copy

base_headers = ['grid', 'year', 'uni_name', 'country', 'region', 'subregion']

dynamic_header = ['data_category', 'data_name', 'data_display_name', 'data_type', 'data_value',
                'data_source', 'data_last_processed', 'data_note']

class CokiProcessor:
    """Base abstract class for a processor

    A processor has a target data name, input data name(s), and process. It iterates over a
    Coki standard long form dataset and generates sums, proportions or other statistical
    aggregations.

    The base class contains the load and save functionality and #TODO. Actual calculations should
    be implemented as subclasses by overwriting the process function.
    """

    def __init__(self, infilepath, target_data_name=None, input_data_names=None, limityears=None):
        self.infilepath = Path(infilepath)
        self.target_data_name = target_data_name
        self.input_data_names = input_data_names
        self.limityears = limityears
        self.output_data = []

        self.data_category = None
        self.data_display_name = None
        self.data_type = None
        self.data_source = None

    def load(self):
        with open(self.infilepath, 'r') as f:
            reader = csv.DictReader(f)
            self.source_data = [line for line in reader]

    def index_gridyears(self):
        if self.limityears:
            self.years = [str(year) for year in range(self.limityears[0], self.limityears[1])]
        else:
            self.years = list(set(line.get('year') for line in self.source_data))
            self.years.sort()
        self.grids = list(set((line.get('grid') for line in self.source_data)))
        self.index = {}
        for year in self.years:
            for grid in self.grids:
                self.index['{}:{}'.format(grid,year)] = [line for line in self.source_data if line['grid'] == grid and line['year'] == year]

    def insert_outputdata(self, result, key, kwargs={}):
        output_data = dict([(field,self.index[key][0][field]) for field in base_headers])
        output_data.update({'data_category' : self.data_category,
                            'data_name' : self.target_data_name,
                            'data_display_name' : self.data_display_name,
                            'data_type' : self.data_type,
                            'data_value' : result,
                            'data_last_processed' : datetime.datetime.today().isoformat(),
                            'data_source' : self.data_source,
                            'data_note' : 'Processed from data source by coki processor'
        })
        output_data.update(**kwargs)
        return output_data

    def process(self):
        raise NotImplementedError
    
    def save(self, outfilepath):
        self.outfilepath = Path(outfilepath)
        with open(outfilepath, 'w') as f:
            writer = csv.DictWriter(f, fieldnames = base_headers + dynamic_header)
            writer.writeheader()
            writer.writerows(self.output_data)

class IntSummer(CokiProcessor):
    """Processor for summing from data sources to produce new composite data
    
    input_data_names must be a list of data_names : Data will be coerced to ints before adding"""

    data_type = 'int'
    collect = True

    def process(self):
        if not self.input_data_names:
            raise ValueError('Need to provide an input data name before calling process')
        for key in self.index.keys():
            self._inner_process(key)

    def _inner_process(self, key):
        data = [int(float(d.get('data_value'))) for d in self.index[key] if d.get('data_name') in self.input_data_names]
        if len(data) > 0:
            self.result = sum([d for d in data if d is not None])
            if self.collect:
                formatted = self.insert_outputdata(self.result, key)
                self.output_data.append(copy.copy(formatted))

        else:
            self.result = None

class Percenter(CokiProcessor):
    """Processor for generating a percentage from data sources to produce new composite data
    
    input_data_names must provide two lists, one to sum items for the numerator, one for the denominator
    
    As the class calls the IntSummer class to generate the numerator and denominator it is preferred to
    also provide target names for each of these. Therefore the recommended means of passing the input_data_names
    is as a dict as follows:
        { 'numerator'   : { 'target_data_name' : <str> target_data_name_for_numerator,
                            'input_data_names' : <list> the list of input data elements}
          'denominator' : { 'target_data_name' : <str> target_data_name_for_numerator,
                            'input_data_names' : <list> the list of input data elements}
        }
    """

    data_type = 'percent'

    def process(self, output_type = 'percent', collectsums = True):
        if not self.input_data_names:
            raise ValueError('Need to provide an input data name before calling process')
        for key in self.index.keys():
            self.numerator = IntSummer(self.infilepath, **self.input_data_names['numerator'])
            if not collectsums:
                self.numerator.collect = False
            self.numerator.index = self.index
            self.numerator._inner_process(key)

            self.denominator = IntSummer(self.infilepath, **self.input_data_names['denominator'])
            if not collectsums:
                self.denominator.collect = False
            self.denominator.index = self.index
            self.denominator._inner_process(key)

            if self.numerator.result and self.denominator.result:
                result = self.numerator.result/self.denominator.result
                if output_type == 'percent':
                    result = result * 100

                formatted = self.insert_outputdata(result, key)
                self.output_data.append(copy.copy(formatted))

            self.output_data.extend(self.numerator.output_data)
            self.output_data.extend(self.denominator.output_data)


