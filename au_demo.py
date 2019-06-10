from databaker.framework import *

path="data/raw/AU_indigenous_raw/2000_staff_gender_indigenous AU.xls"
tab, = loadxlstabs(path, "5", verbose = False)
#print (tab)

top_headers = tab.filter(lambda cell: cell.y <=10)
institutions = tab.filter("State/Institution").fill(DOWN)
genders = top_headers.filter("Males") | top_headers.filter("Females") | top_headers.filter("All")
aggregate = top_headers.filter("Estimated Casual") | top_headers.filter("TOTAL FTE")
fte = top_headers.filter("Full-time") | top_headers.filter("Fractional Full-time") | top_headers.filter("Full-time plus Fractional Full-time")
none = top_headers.filter(contains_string("Table"))

#print (institutions)
#print (genders)
#print (fte)



dimensions = [
    HDimConst(DATAMARKER, 4),
    HDim(institutions, "Institution", DIRECTLY, LEFT),
    HDim(genders, "Gender", DIRECTLY, ABOVE),
    HDim(fte, "FT/PT", CLOSEST, LEFT),
    HDimConst(TIME, 2000)
]

observations = institutions.expand(RIGHT).is_not_blank().is_not_whitespace()
c1 = ConversionSegment(observations, dimensions)

print(c1.topandas())
