# encoding=utf-8
from databaker.framework import *
import sa_walk
import feather
path = "data/raw/nz_raw_data/NZ 0321_Universities_Workforce_Data_gender_ethnicity 2000-2017.xlsx"

tab, = loadxlstabs(path, "Staff type x Age x Gender", verbose = False)
anchor = tab.filter_one("Age Group")
#race = anchor.fill(DOWN)
gender = anchor.fill(RIGHT)
#institution = tab.filter("Provider").fill(DOWN).is_not_blank()
profession = tab.filter("Staff Designation").fill(DOWN).is_not_blank()
# fte = tab.filter("Part-time").expand(RIGHT).is_not_blank()
# metric = tab.filter("FTE") | tab.filter("Number of Staff")
year = tab.excel_ref("D5").fill(RIGHT).is_not_blank()
age = anchor.fill(DOWN)
observations = age.fill(RIGHT).is_not_blank()

dimensions = [
    HDimConst(DATAMARKER, 0),
    HDimConst(TIME, 0),
    #HDim(institution, "Institution", CLOSEST, ABOVE),
    HDim(profession, "Profession", CLOSEST, ABOVE),
    #HDim(race, "Race", DIRECTLY, LEFT),
    HDim(gender, "Gender", DIRECTLY, ABOVE),
#    HDim(fte, "fte", CLOSEST, LEFT),
    #HDim(metric, "Metric", CLOSEST, LEFT),
#    HDim(stat, "Statistic", CLOSEST, ABOVE),
    HDim(age, "Age", DIRECTLY, LEFT)
    HDim(year, "Time", CLOSEST, LEFT)
]

c1 = ConversionSegment(observations, dimensions)
df = c1.topandas()
feather.write_dataframe(df, f"feather/NZ-staff-age-gen.feather") # TODO won't get all!
df.to_csv(f"csv/NZ-staff-age-gen.csv", header=True)
print(df)
