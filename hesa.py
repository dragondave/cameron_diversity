import os
import re
from pathlib import Path
from collections import namedtuple
from databaker.framework import *
import feather
import re

def walk_hesa():
    path = "data/selected/uk_hesa"
    for directory, directories, files in os.walk(path):
        for file in files:
            if ".pdf" not in file and ".csv" not in file:
                yield Path(directory) / Path(file)

def get_table(path):
    if "_2abc" in str(path):
        tab, = loadxlstabs(path, "Table_2a", verbose=False)
        yield tab
        tab, = loadxlstabs(path, "Table_2b", verbose=False)
        yield tab
        tab, = loadxlstabs(path, "Table_2c", verbose=False)
        yield tab

    else:
        tab, = loadxlstabs(path, "*", verbose=False)
        yield tab

for xls_path in walk_hesa():
    year, = re.findall("\d\d\d\d", str(xls_path))
    print (year, xls_path)
    for table in get_table(xls_path):  # 1 or 3 depending on yea
        if "..." # TODO

