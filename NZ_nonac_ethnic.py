# encoding=utf-8
from databaker.framework import *
import sa_walk
import feather
path = "data/raw/nz_raw_data/NZ_Non_Academic_Staff_Workforce_Data.xlsx"

tab, = loadxlstabs(path, "Ethnic Group", verbose = False)
anchor = tab.filter("Ethnic Group")
gender = anchor.fill(RIGHT)
ethnic = anchor.fill(DOWN)
subsector = tab.filter("Sub-sector").fill(DOWN).is_not_blank()
fte = anchor.shift(UP).fill(RIGHT).is_not_blank()
year = anchor.shift(UP).shift(UP).fill(RIGHT).is_not_blank()

observations = ethnic.fill(RIGHT).is_not_blank()

dimensions = [
    HDimConst(DATAMARKER, 0),
    HDim(subsector, "Sub-sector", CLOSEST, ABOVE),
    HDimConst("IsAcademic", "Non-Academic"),
    HDim(fte, "FTE", CLOSEST, LEFT),
    HDim(gender, "Gender", DIRECTLY, ABOVE),
    HDim(ethnic, "Ethnic Group", DIRECTLY, LEFT),
    HDim(year, "Year", CLOSEST, LEFT),
]

c1 = ConversionSegment(observations, dimensions)
df = c1.topandas()
feather.write_dataframe(df, f"feather/NZ-nonac-ethnic.feather") # TODO won't get all!
df.to_csv(f"csv/NZ-nonac-ethnic.csv", header=True)
print(df)
