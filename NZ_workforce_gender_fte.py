# encoding=utf-8
from databaker.framework import *
import sa_walk
import feather
path = "data/raw/nz_raw_data/NZ 0321_Universities_Workforce_Data_gender_ethnicity 2000-2017.xlsx"

tab, = loadxlstabs(path, "Gender x Full-Part Time", verbose = False)
tenure = tab.filter_one("Tenure")
fte = tenure.fill(DOWN)
gender = tenure.fill(RIGHT)
institution = tab.filter("Provider").fill(DOWN).is_not_blank()
stat = tab.filter("Number of academic staff") | tab.filter("Full-time Equivalent staff (FTE)")
year = tab.excel_ref("D4").fill(RIGHT).is_not_blank()
observations = fte.fill(RIGHT).is_not_blank()

dimensions = [
    HDimConst(DATAMARKER, 0),
    HDimConst(TIME, 0),
    HDim(institution, "Institution", CLOSEST, ABOVE),
    HDim(gender, "Gender", DIRECTLY, ABOVE),
    #HDim(race, "Race", DIRECTLY, LEFT),
    HDimConst("IsAcademic", "Academic"),
    HDim(fte, "fte", DIRECTLY, LEFT),
    HDim(stat, "Statistic", CLOSEST, ABOVE),
    HDim(year, "Time", CLOSEST, LEFT)
]

c1 = ConversionSegment(observations, dimensions)
df = c1.topandas()
feather.write_dataframe(df, f"feather/NZ-gender-fte.feather") # TODO won't get all!
df.to_csv(f"csv/NZ-gender-fte.csv", header=True)
print(df)
