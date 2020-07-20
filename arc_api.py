#!/usr/bin/env python
# coding: utf-8


import json
import pandas as pd
import datetime
import requests
import json
import os.path

#address of api with parameters put in
api = ('https://services.arcgis.com/PwY9ZuZRDiI5nXUB/ArcGIS/rest/services/ADH_COVID19_Positive_Test_Results/FeatureServer/0/query?where=0%3D0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=county_nam%2C+positive%2C+negative%2C+Recoveries%2C+deaths%2C+total_tests%2C+active_cases&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token=')

#requesting the web path above
r = requests.get(api)

#is the api avaliable?
if r.status_code == 200:
    print("Connection Avaliable")
elif r.status_code == 404:
    print('Not Found')
else:
    print('Error')


#convert json obj into pandas df
df = pd.json_normalize(r.json()['features'])


#set fields to look for, nested spots in the json
FIELDS = ["attributes.county_nam","attributes.positive","attributes.negative","attributes.Recoveries","attributes.deaths","attributes.total_tests","attributes.active_cases"]

#only pick the specified features
df = df[FIELDS]

#rename the columns
df.columns = ['County', 'Positive','Negative','Recovered','Total Deaths','Number Tested','Current Infections']

# add a date column
day = datetime.date.today()
df["Date"] = day

#reoreder them
df = df[['County','Date','Positive','Negative','Recovered','Total Deaths','Number Tested','Current Infections']]

df = df.sort_values("County")

# check if ARC_master exists, if not, make it
if os.path.exists(r'arc_master.csv'):
    print("Found arc_master.csv")
    # Load master, append daily to master, and remove dublicates
    master = pd.read_csv(r'arc_master.csv')
    
    #check if df is different from master
    print("Checking if data has been updated.")
    if len(df.merge(master[["County",'Positive','Negative','Recovered','Total Deaths','Number Tested','Current Infections']]).drop_duplicates()[["County",'Positive','Negative','Recovered','Total Deaths','Number Tested','Current Infections']]) == len(df.drop_duplicates()[["County",'Positive','Negative','Recovered','Total Deaths','Number Tested','Current Infections']]):
        print("Data has not been updated, aborting.")
        quit()
    
    else:
        print("Concatenating files")

        # Drop all rows with a ["Date"] of date.today()
        master = master[master.Date != day.isoformat()]

        master = pd.concat([df, master])
        master = master.drop_duplicates()

        master.to_csv(r'arc_master.csv',encoding='utf-8', index = False, header = True)
        
        #run upload script
        os.system("python3 scripts/UploadCsvtoSheets.py 1")
else:
    print("Creating new arc_master.csv")
    df.to_csv(r'arc_master.csv',encoding='utf-8', index = False, header = True)

