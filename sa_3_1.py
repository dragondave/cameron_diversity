from databaker.framework import *
import sa_walk

files = [x for x in sa_walk.SA_files() if x.table=="3.1"]

for year, _, filename in files:
    YEAR = int(year)
    # path=f"data/raw/South Africa Staff Tables_raw/{YEAR} Staff tables for Universities/{YEAR} Table 3.3 for Universities.xls"

    for tab in loadxlstabs(filename, "*", verbose = False):
        institution = tab.filter_one(lambda cell: cell.x==0 and cell.y==1).value
        # TODO -- varies from A1 to A2 across 2001/2017
        if not institution:  # Further tidying required -- INSTITUTION: H09 (UNIVERSITY OF LIMPOPO)
            institution = "National"
        else:
            institution = institution.split("  ")[-1].strip()
        print (institution)

        anchor = tab.filter_one("PERSONNEL CATEGORY")
        programme = anchor.fill(DOWN).is_not_blank()
        profcat = anchor.fill(RIGHT).is_not_blank()
        observations = programme.fill(RIGHT).is_not_blank()

        dimensions = [
            HDimConst(DATAMARKER, 0),
            HDimConst("Institution", institution)
            HDim(programme, "Programme", DIRECTLY, LEFT),
            HDim(profcat, "Personnel Category", DIRECTLY, ABOVE),
            HDimConst(TIME, YEAR)
        ]

        c1 = ConversionSegment(observations, dimensions)
        print(c1.topandas())

