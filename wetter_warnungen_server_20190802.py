# -*- coding: utf-8 -*-
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import geopandas as gpd
import pandas as pd
import os
import datetime
import pygsheets

#authorization
os.chdir('D:/suchdialog_google_drive/Wetterdaten_/')
gauth = GoogleAuth()
gauth.LocalWebserverAuth() # Creates local webserver and auto handles authentication.

gc = pygsheets.authorize(service_file='D:/suchdialog_google_drive/Wetterdaten_/save_googlesheet.json')

# Create GoogleDrive instance with authenticated GoogleAuth instance.
drive = GoogleDrive(gauth)

# Read geotargets.
file = drive.CreateFile({'id': '1lB2_d8z8sSQAvRh2oQjjpSvkIuHPmRaA'})
file.GetContentFile('geotargets.csv') 
google_id_df_filled = pd.read_csv('geotargets.csv').drop(columns=['Unnamed: 0'])

geo_google_id_df = gpd.GeoDataFrame(google_id_df_filled, geometry=gpd.points_from_xy(google_id_df_filled.Longitude, google_id_df_filled.Latitude))
geo_google_id_df.crs = {'init' :'epsg:4326'}
 
# read entire map kreise
file2 = drive.CreateFile({'id': '1tkhDiyW4eHp9IGyFmH4iO9_F1lrj701h'})
file2.GetContentFile('landkreise.shp') 
file3 = drive.CreateFile({'id': '1RE5rCRsw_hRH7nZkNLYljEGbPiN63_eW'})
file3.GetContentFile('landkreise.shx') 
file4 = drive.CreateFile({'id': '1vh5JNv8UvmIKWXRecHrvzWnY8qSrlbj6'})
file4.GetContentFile('landkreise.cpg') 
file5 = drive.CreateFile({'id': '156d84fK3IuC_comNOnO7f4nPt9b-P1Zm'})
file5.GetContentFile('landkreise.dbf') 
file6 = drive.CreateFile({'id': '1YQh84mF2qiiHX0j_fcuZa-xkWaLOtPZK'})
file6.GetContentFile('landkreise.prj') 

df_kreise = gpd.read_file('landkreise.shp')
df_kreise.crs = {'init' :'epsg:4326'}

# match the two
matched_kreise = gpd.sjoin(geo_google_id_df, df_kreise, how='right')

# treatment
test_kreise = matched_kreise.groupby('WARNCELLID').index_left.count().value_counts().sort_index()
treat_kreise = matched_kreise.groupby('WARNCELLID').apply(lambda x: x.sample(frac=0.5, random_state=1))['index_left'].to_frame()
treat_kreise['treatment']=1

matched_kreise = matched_kreise.merge(treat_kreise, on='index_left', how='left') 
matched_kreise['treatment'] = matched_kreise.treatment.fillna(value=0)
#matched_kreise.groupby('treatment').count()
#points_kreise = matched_kreise[['Longitude','Latitude','treatment','Name']].dropna()
#geo_points_kreise = gpd.GeoDataFrame(points_kreise, geometry=gpd.points_from_xy(points_kreise.Longitude, points_kreise.Latitude))
#geo_points_kreise.crs = {'init' :'epsg:4326'}

# wieviele geocodes innerhalb eines Kreises / einer gemeinde
# danach: treatment / control definieren

# warnungs files
filelist = [#{'name':'Warnungen_Gemeinden',
            # 'match':matched_gemeinden,
            # 'id':'WARNCELLID',
            # 'points':geo_points_gemeinden,
            # 'list':['Criteria ID','Name', 'WARNCELLID', 'NAME_x','treatment', 'SENT', 'STATUS','MSGTYPE','CATEGORY', 'EVENT', 'RESPONSETYPE', 'URGENCY', 'SEVERITY', 'CERTAINTY', 'EC_GROUP','EFFECTIVE', 'ONSET', 'EXPIRES', 'HEADLINE','DESCRIPTION', 'INSTRUCTION','PARAMETERNAME','PARAMETERVALUE']},
            #{'name':'Warnungen_Gemeinden_vereinigt',
            # 'match':df_gemeinden},
             {'name':'Warnungen_Landkreise',
             'match':matched_kreise,
             'id':'GC_WARNCELLID',
             #'points':geo_points_kreise,
             'list':['sample','treatment','treatment_unwetter','download_time','WARNCELLID','NAME','Criteria ID','Name','PROCESSTIM','SENT', 'STATUS','MSGTYPE','PROCESSTIME', 'CATEGORY', 'EVENT', 'RESPONSETYPE', 'URGENCY', 'SEVERITY', 'CERTAINTY', 'EC_GROUP','EFFECTIVE', 'ONSET', 'EXPIRES', 'HEADLINE','DESCRIPTION', 'INSTRUCTION','PARAMETERNAME','PARAMETERVALUE']}]

EC_list = [13,15,16,31,33,34,36,38,40,41,42,44,45,46,48,49,52,53,54,55,56,58,62,66,73,78,90,91,92,93,95,96]
# read warnungen
for file in filelist:
    print(file['name'])
    url = 'https://maps.dwd.de/geoserver/dwd/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=dwd%3A'+file['name']+'&outputFormat=application%2Fjson'
    df = gpd.read_file(url)
    time_now = datetime.datetime.now().strftime("%Y%m%d_%H_%M_%S")
    # merge with entire map
    if df.shape[0]!=0:
        df.to_csv(file['name']+'_'+time_now+'.csv',sep=';')
        uploaded = drive.CreateFile({'title':'temp_'+file['name']+'_'+time_now+'.csv'})
        uploaded = drive.CreateFile({'title': 'name.csv'})
        uploaded.SetContentFile(file['name']+'_'+time_now+'.csv')
        uploaded.Upload()
        merged = file['match'].merge(df.drop(columns=['geometry']), left_on = 'WARNCELLID', right_on = file['id'], how = 'outer')
    merged['Wetterwarnung'] = merged.EVENT.notnull()
    trigger = merged[merged['Wetterwarnung']==True]
    trigger = trigger[trigger['STATUS']=='Actual']
    trigger = trigger[trigger['MSGTYPE'].isin(['Alert','Update'])]
    #trigger = trigger[trigger['RESPONSETYPE']!='AllClear']
    trigger = trigger[trigger['URGENCY']=='Immediate']
    trigger = trigger[trigger['CERTAINTY']=='Observed']
    trigger = trigger[trigger['EC_II'].isin(EC_list)]
    trigger['treatment_unwetter'] = 1
    trigger = trigger[['Criteria ID','treatment_unwetter']]
    final = merged.merge(trigger,on='Criteria ID',how='outer')
    final['download_time'] = time_now
    final = final[file['list']]
    final.to_csv('temp_recent_'+file['name']+'.csv',sep=';')
    sh = gc.open_by_key('1IFySHsmDlEdQyzQh1WHXVr9Qfe7yN8qghBswa1Ek_Xc')
    wks = sh.worksheet_by_title('_Geotargetmapping_')
    wks.set_dataframe(final,(1,1))
    