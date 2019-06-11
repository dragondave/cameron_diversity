from databaker.framework import *
import sa_walk
import re
import feather
table = "3.7"
files = [x for x in sa_walk.SA_files() if x.table==table]

for year, _, filename in files:
    try:
        YEAR = int(year)
    except:
        print (year, filename)
    # path=f"data/raw/South Africa Staff Tables_raw/{YEAR} Staff tables for Universities/{YEAR} Table 3.3 for Universities.xls"

    for tab in loadxlstabs(filename, "*", verbose = False):
        institution = tab.filter_one(re.compile("INSTITUTION: .*")).value
        if not institution:  # Further tidying required -- INSTITUTION: H09 (UNIVERSITY OF LIMPOPO)
            institution = "National"
        else:
            institution = institution.split("  ")[-1].strip()
        print (institution)


        anchor = tab.filter("STAFF PROGRAMME BY CESM") | tab.filter("Staff Programme CESM")
        assert len(anchor) == 1
        programme = anchor.fill(DOWN)
        profcat = anchor.fill(RIGHT)
        observations = programme.fill(RIGHT).is_not_blank()

        dimensions = [
            HDimConst(DATAMARKER, 0),
            HDimConst("Institution", institution),
            HDim(programme, "Programme", DIRECTLY, LEFT),
            HDim(profcat, "Profession", DIRECTLY, UP),
            HDimConst(TIME, YEAR)
        ]

        c1 = ConversionSegment(observations, dimensions)
        df = c1.topandas()

        ins = institution.replace(":", "_").replace("/", "_").replace("\\", "_")
        feather.write_dataframe(df, f"feather/{YEAR}-{ins}-{table}.feather") # TODO won't get all!
        df.to_csv(f"csv/{YEAR}-{ins}-{table}.csv", header=True)
        print(c1.topandas())
