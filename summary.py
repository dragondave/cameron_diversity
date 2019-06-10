filename = "feather/2015-3_1.feather"

import os
import feather

collection = {}

def get_headers(filename):
    df = feather.read_dataframe(filename)
    headers = [x for x in list(df) if x not in ["OBS", "DATAMARKER", "YEAR", "TIME", "TIMEUNIT"]]
    for header in headers:
        values = set(df[header])
        if header in collection:
            collection[header] = collection[header] | values
        else:
            collection[header] = values

for _, _, filenames in os.walk("feather"):
    for filename in filenames:
        headers = get_headers(f"feather/{filename}")

for header in collection:
    print (f"=={header}==")
    for item in collection[header]:
        print (item)
    print()


