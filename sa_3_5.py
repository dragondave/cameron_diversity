from databaker.framework import *
import sa_walk
import re
import feather

table = "3.5"
files = [x for x in sa_walk.SA_files() if x.table==table]

for year, _, filename in files:
    YEAR = int(year)
    # path=f"data/raw/South Africa Staff Tables_raw/{YEAR} Staff tables for Universities/{YEAR} Table 3.3 for Universities.xls"

    for tab in loadxlstabs(filename, "*", verbose = False):
        institution = tab.filter_one(re.compile("INSTITUTION: .*")).value 
        print (institution)


        jobtitles = tab.filter("NOTES").fill(RIGHT).is_not_blank()
        genders = tab.filter("M") | tab.filter("F") | tab.filter("U") | tab.filter("T")
        try:
            ages = tab.filter_one(re.compile("(?:Age|AGE)")).fill(DOWN)
        except:
            print (filename)
            raise
        observations = ages.fill(RIGHT).is_not_blank()

        dimensions = [
            HDimConst(DATAMARKER, 0),
            HDimConst("Institution", institution),
            HDim(genders, "Gender", DIRECTLY, ABOVE),
            HDim(ages, "Age", DIRECTLY, LEFT),
            HDim(jobtitles, "Profession", CLOSEST, LEFT),
            HDimConst(TIME, YEAR)
        ]

        c1 = ConversionSegment(observations, dimensions)
        df = c1.topandas()

        ins = institution.replace(":", "_").replace("/", "_").replace("\\", "_")
        feather.write_dataframe(df, f"feather/{YEAR}-{ins}-{table}.feather") # TODO won't get all!
        df.to_csv(f"csv/{YEAR}-{ins}-{table}.csv", header=True)
        print(c1.topandas())
