from databaker.framework import *
import sa_walk

files = [x for x in sa_walk.SA_files() if x.table=="3.3"]

for year, _, filename in files:
    YEAR = int(year)
    # path=f"data/raw/South Africa Staff Tables_raw/{YEAR} Staff tables for Universities/{YEAR} Table 3.3 for Universities.xls"

    for tab in loadxlstabs(filename, "*", verbose = False):
        institution = tab.filter_one(lambda cell: cell.x==0 and cell.y==1).value
        if not institution:  # Further tidying required -- INSTITUTION: H09 (UNIVERSITY OF LIMPOPO)
            institution = "National"
        else:
            institution = institution.split("  ")[-1].strip()
        print (institution)


        race = tab.filter("WHITE") | tab.filter("COLOURED") | tab.filter("INDIAN") | tab.filter("AFRICAN") | tab.filter("ALL OTHER") | tab.filter("TOTAL")
        genders = tab.filter("MALE") | tab.filter("FEMALE") | tab.filter("UNKNOWN") | tab.filter("TOTAL")
        profcat = tab.filter_one("PERSONNEL CATEGORY").fill(DOWN)
        observations = profcat.fill(RIGHT).is_not_blank()

        dimensions = [
            HDimConst(DATAMARKER, 0),
            HDimConst("Institution", institution),
            HDim(genders, "Gender", DIRECTLY, ABOVE),
            HDim(race, "Race", DIRECTLY, ABOVE),
            HDim(profcat, "Profession", DIRECTLY, LEFT),
            HDimConst(TIME, YEAR)
        ]

        c1 = ConversionSegment(observations, dimensions)

        print(c1.topandas())

