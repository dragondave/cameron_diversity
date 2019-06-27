# encoding=utf-8
from databaker.framework import *
import sa_walk
import feather
path = "data/raw/nz_raw_data/NZ_Non_Academic_Staff_Workforce_Data.xlsx"

tab, = loadxlstabs(path, "Staff Designation", verbose = False)
anchor = tab.filter("Gender")
gender = anchor.fill(DOWN)
subsector = tab.filter("Sub-sector").fill(DOWN).is_not_blank()
designation = tab.filter("Staff Designation").fill(DOWN).is_not_blank()
fte = anchor.fill(RIGHT)
year = anchor.shift(UP).fill(RIGHT).is_not_blank()

observations = gender.fill(RIGHT).is_not_blank()

dimensions = [
    HDimConst(DATAMARKER, 0),
    HDim(subsector, "Sub-sector", CLOSEST, ABOVE),
    #HDim(genders, "Gender", DIRECTLY, ABOVE),
    HDim(designation, "Staff Designation", CLOSEST, ABOVE),
    HDimConst("IsAcademic", "Non-Academic"),
    HDim(fte, "FTE", DIRECTLY, ABOVE),
    HDim(gender, "Gender", DIRECTLY, LEFT),
    HDim(year, "Year", CLOSEST, LEFT),
]

c1 = ConversionSegment(observations, dimensions)
df = c1.topandas()
feather.write_dataframe(df, f"feather/NZ-nonac-staff.feather") # TODO won't get all!
df.to_csv(f"csv/NZ-nonac-staff.csv", header=True)
print(df)
