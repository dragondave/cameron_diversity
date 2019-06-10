from databaker.framework import *
import sa_walk
import re

files = [x for x in sa_walk.SA_files() if x.table=="3.5"]

for year, _, filename in files:
    YEAR = int(year)
    # path=f"data/raw/South Africa Staff Tables_raw/{YEAR} Staff tables for Universities/{YEAR} Table 3.3 for Universities.xls"

    for tab in loadxlstabs(filename, "*", verbose = False):
        institution = tab.filter_one(lambda cell: cell.x==0 and cell.y==0).value # Note: different to 3_3! Yuck!
        if not institution:  # Further tidying required -- INSTITUTION: H09 (UNIVERSITY OF LIMPOPO)
            institution = "National"
        else:
            institution = institution.split("  ")[-1].strip()
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

        print(c1.topandas())

