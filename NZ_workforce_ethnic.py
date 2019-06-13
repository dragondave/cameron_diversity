# encoding=utf-8
from databaker.framework import *
import sa_walk
import feather
path = "data/raw/nz_raw_data/NZ 0321_Universities_Workforce_Data_gender_ethnicity 2000-2017.xlsx"

tab, = loadxlstabs(path, "Ethnic Group", verbose = False)
race = tab.filter("Ethnic Group").fill(DOWN)
institution = tab.filter("Provider").fill(DOWN).is_not_blank()
year = tab.filter("Ethnic Group").fill(RIGHT)
month = tab.filter("Snapshot -1st week in August") | tab.filter("y.e December")


observations = race.fill(RIGHT).is_not_blank()

dimensions = [
    HDimConst(DATAMARKER, 0),
    HDim(institution, "Institution", CLOSEST, ABOVE),
    #HDim(genders, "Gender", DIRECTLY, ABOVE),
    HDim(race, "Race", DIRECTLY, LEFT),
    HDimConst("Profession", "Academic"),
    HDim(year, TIME, DIRECTLY, ABOVE),
    HDim(month, "Month", CLOSEST, LEFT),
]

c1 = ConversionSegment(observations, dimensions)
df = c1.topandas()
feather.write_dataframe(df, f"feather/NZ-ethnic.feather") # TODO won't get all!
df.to_csv(f"csv/NZ-ethnic.csv", header=True)
print(df)
