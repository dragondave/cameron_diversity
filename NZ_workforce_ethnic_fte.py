# encoding=utf-8
from databaker.framework import *
import sa_walk
import feather
path = "data/raw/nz_raw_data/NZ 0321_Universities_Workforce_Data_gender_ethnicity 2000-2017.xlsx"

tab, = loadxlstabs(path, "Ethnic x Full-Part Time", verbose = False)
anchor = tab.filter_one("Ethnic Group")
race = anchor.fill(DOWN)
gender = anchor.fill(RIGHT)
institution = tab.filter("Provider").fill(DOWN).is_not_blank()
profession = tab.filter("Staff Type/Group").fill(DOWN).is_not_blank()
fte = tab.filter("Part-time").expand(RIGHT).is_not_blank()
year = tab.excel_ref("D5").fill(RIGHT).is_not_blank()
observations = race.fill(RIGHT).is_not_blank()

dimensions = [
    HDimConst(DATAMARKER, 0),
    HDimConst(TIME, 0),
    HDim(institution, "Institution", CLOSEST, ABOVE),
    HDim(profession, "Profession", CLOSEST, ABOVE),
    HDimConst("IsAcademic", "Academic"),
    HDim(race, "Race", DIRECTLY, LEFT),
    HDim(gender, "Gender", DIRECTLY, ABOVE),
    HDim(fte, "fte", CLOSEST, LEFT),
#    HDim(stat, "Statistic", CLOSEST, ABOVE),
    HDim(year, "Time", CLOSEST, LEFT)
]

c1 = ConversionSegment(observations, dimensions)
df = c1.topandas()
feather.write_dataframe(df, f"feather/NZ-ethnic-fte.feather") # TODO won't get all!
df.to_csv(f"csv/NZ-ethnic-fte.csv", header=True)
print(df)
