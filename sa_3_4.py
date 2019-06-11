from databaker.framework import *
import sa_walk
import feather
import re

table = "3.4"
files = [x for x in sa_walk.SA_files() if x.table==table]

for year, _, filename in files:
    YEAR = int(year)
    # path=f"data/raw/South Africa Staff Tables_raw/{YEAR} Staff tables for Universities/{YEAR} Table 3.3 for Universities.xls"

    for tab in loadxlstabs(filename, "*", verbose = False):
        institution = tab.filter_one(re.compile("INSTITUTION: .*")).value
        # TODO -- varies from A1 to A2 across 2001/2017
        if not institution:  # Further tidying required -- INSTITUTION: H09 (UNIVERSITY OF LIMPOPO)
            institution = "National"
        else:
            institution = institution.split("  ")[-1].strip()
        print (institution)

        rank = tab.filter_one("PROFESSOR").expand(RIGHT)
        qual = tab.filter_one("HIGHEST MOST RELEVANT QUALIFICATION").fill(DOWN).is_not_blank()
        observations = qual.fill(RIGHT).is_not_blank()

        dimensions = [
            HDimConst(DATAMARKER, 0),
            HDimConst("Institution", institution),
            HDim(rank, "Rank", DIRECTLY, ABOVE),
            HDim(qual, "Highest Qual", DIRECTLY, LEFT),
            HDimConst(TIME, YEAR)
        ]

        c1 = ConversionSegment(observations, dimensions)
        print(c1.topandas())
        df = c1.topandas()
        ins = institution.replace(":", "_").replace("/", "_").replace("\\", "_")
        feather.write_dataframe(df, f"feather/{YEAR}-{ins}-{table}.feather") # TODO won't get all!
        df.to_csv(f"csv/{YEAR}-{ins}-{table}.csv", header=True)
