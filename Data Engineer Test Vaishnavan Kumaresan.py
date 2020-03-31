#!/usr/bin/env python
# coding: utf-8


import os
import glob
import pandas as pd
import pandasql as ps

#specified file directory
os.chdir("D:/Downloads moved to storage/Data Engineer Test")

#look for csv weather files and combine csv files
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])

combined_csv.to_csv( "combined_weather_csv.csv", index=False, encoding='utf-8-sig')

#convert combined csv file into parquet file
df = pd.read_csv('D:/Downloads moved to storage/Data Engineer Test/combined_weather_csv.csv')
df.to_parquet('combined_weather.parquet') 

#removed combined csv file to save storage space, clean up directory
os.remove("D:/Downloads moved to storage/Data Engineer Test/combined_weather_csv.csv")

#converted to dataframe
df_final = pd.read_parquet('D:/Downloads moved to storage/Data Engineer Test/combined_weather.parquet', engine='pyarrow')
 
#have not specified datatypes for dataframe as question didn't need prepped data in this instance.


#used pandasql to print required fields via readable query
hottest_day_temp_region = ps.sqldf("SELECT ObservationDate, ScreenTemperature, Region from df_final where ScreenTemperature = (SELECT MAX(ScreenTemperature) from df_final)")

print(hottest_day_temp_region)





