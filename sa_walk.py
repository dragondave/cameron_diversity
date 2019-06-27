import os
import re
from pathlib import Path
from collections import namedtuple

File = namedtuple("File", ["year", "table", "filename"])

def SA_files():
    path = "data/raw/South Africa Staff Tables_raw"
    for directory, directories, files in os.walk(path):
        for file in files:
            filename = Path(directory) / Path(file)
            year = file[:4]
            try:
                table = re.search("[ _](3.\d)[ _.]", file).group(1)
            except:
                print (file)
            if str(filename).endswith(".rtf"):
                continue
            yield File(year, table, filename)

def AU_files():
    path = "data/raw/AU_indigenous_raw"
    for directory, directories, files in os.walk(path):
        for file in files:
            filename = Path(directory) / Path(file)
            year = file[:4]
            if year == "AU_i": continue
            yield File(year, "*", filename)
    

#print (list(SA_files()))
