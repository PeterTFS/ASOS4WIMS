#-------------------------------------------------------------------------------
# Name:        ASOS2WIMS_TX_HISTORY.py
# Purpose:     Process downloaded historical ASOS hourly record to FW9 forwat
# Author:      pyang
# Created:     11/4/2015
#-------------------------------------------------------------------------------
import time
import os
import sys
import re
import csv
import string
import urllib
import shutil
import pandas
import math
#import multiprocessing
import logging
from datetime import date, datetime
from dateutil import tz
import smtplib


#For the season code:input an actual day to get the season code
Y = 2000 # dummy leap year to allow input X-02-29 (leap day)
seasons = [(1, (date(Y,  1,  1),  date(Y,  3, 20))),
           (2, (date(Y,  3, 21),  date(Y,  6, 20))),
           (3, (date(Y,  6, 21),  date(Y,  9, 22))),
           (4, (date(Y,  9, 23),  date(Y, 12, 20))),
           (1, (date(Y, 12, 21),  date(Y, 12, 31)))]

#-----------------------------------------------------------------------------------------
# Convert UTC to Central Zone (can be any zone)
#-----------------------------------------------------------------------------------------
def UTC2LOCAL(TIMESTR):
    from_zone = tz.tzutc()
    #to_zone = tz.tzlocal() #changed to local zone
    to_zone = tz.gettz("US/Central") #Need to change if other than TX
    utc = datetime.strptime(TIMESTR,"%Y-%m-%d %H:%M")
    utc=utc.replace(tzinfo=from_zone)
    local = utc.astimezone(to_zone)
    return local


def get_season(now):
    if isinstance(now, datetime):
        now = now.date()
    now = now.replace(year=Y)
    return next(season for season, (start, end) in seasons
                if start <= now <= end)


# Processing the 3 level of sky cover into one column
def SKY(series):
    return series[' skyc1'] + series[' skyc2'] + series[' skyc3']

#--------------------------------------------------------------------------------------------------------------------------------------------------------
# ASOS wind sensors are at a height of 10 meters, but the RAWS/WIMS standard is for 6 meter/20 foot winds.
#To estimate the 6 meter wind speed from the 10 meter measurement, the logarithmic wind profile method  math.log(6/0.0984)/math.log(10/0.0984) can be used
#To convert the knot to mph, the 1.15078 ration is used. Mike suggest mannually reduce the windspeed by 10% (*0.9) for WIMS
#---------------------------------------------------------------------------------------------------------------------------------------------------------
def windspeed(wind_speed_kt):
    return wind_speed_kt * 0.9 * math.log(6/0.0984)/math.log(10/0.0984) * 1.15078

#Define a precipitation hous based on the measurable amount of precipitation
def precipDuration(preci_in):
    if preci_in > 0:
        return 1
    else:
        return 0

def formatPrecip(p):
    return round(p, 2)

#---------------------------------------------------------------------------
#Function to remove the pre-24 hour observation if there are missing records
#---------------------------------------------------------------------------
def checkMissingRecords(df):
    #check time difference for each hour is 1 hour and for the first and last record is 24
    timeBegin = datetime.strptime(df.iloc[0]['valid'],"%Y-%m-%d %H:%M")
    #print 'timeBegin:',timeBegin
    for index,row in df.iterrows():
        timeEnd   = datetime.strptime(row['valid'],"%Y-%m-%d %H:%M")
        tdelta =  timeBegin - timeEnd
        if(tdelta.seconds/3600 > 23):
            df = df.drop(index)
            print "There is a missing record for Station: ",(row['station'])
    return df

#Format the float number to int
def formatFloat(v):
    return int(round(v))
#--------------------------------------------------------------------
# Function for Formatting the extracted information to the wf9 format
# Input: Dictionary (X) that contains all the information
# Output: A string with fw9 format
#--------------------------------------------------------------------
def FormatFW9( X ):
    Fields = (('W98',(0,'3A')),('Station Number',(3,'6A')),('Ob Date',(9,'8A')),('Ob Time',(17,'4A')),
              ('Type',(21,'1A')),('State of Weather',(22,'1N')),('Temp',(23,'3N')),('Moisture',(26,'3N')),
              ('WindDir',(29,'3N')),('WindSpeed',(32,'3N')),('10hr Fuel',(35,'2N')),('Tmax',(37,'3N')),
              ('Tmin',(40,'3N')),('RHmax',(43,'3N')),('RHmin',(46,'3N')),('PrecipDur',(49,'2N')),
              ('PrecipAmt',(51,'5N')),('WetFlag',(56,'1A')),('Herb',(57,'2N')),('Shrub',(59,'2N')),
              ('MoistType',(61,'1N')),('MeasType',(62,'1N')),('SeasonCode',(63,'1N')),('SolarRad',(64,'4N'))
              )
    Out = []
    for f,p in Fields:
        val = X[f]
        #str(X[f]).zfill()
        length = int(p[1][:-1]) #not working
        format = p[1][-1]
        #This is the total precipitation in the previous 24 hours, given in thousands of an inch. For
        #example, an observation of 0.04? would be entered as ___40, preceded by three
        #blanks/spaces. An observation of 1.25? would be entered as _1250, preceded by one space.
        #An observation of no rainfall would be entered as all blanks/spaces.
        if f=='PrecipAmt':
            if val == 0:
                val=-999
            else:
                val*=1000
        WindParaList = ['WindSpeed','WindDir','Temp','Tmax','Tmin','Moisture','RHmax','RHmin','10hr Fuel']#If those paras are missing value, make it a blank
        if f in WindParaList :
            #print 'Val:',val
            if val == 0:
               val=-999
        else:
            ZeroPad = ''
        if format == 'N' and val != -999:
            #q = str(0).zfill(length)
            q = '%%%s%dd' % (ZeroPad,length)
        elif format == 'N' and val == -999:
            val = ' '
            q = '%%%s%ds' % (ZeroPad,length)
        else:
            q = '%%%ds' % length
        try:
            Out.append( q % val )
        except:
            print f, p, q, val, type(val)
    return string.join( Out, '' )

#--------------------------------------------------------------------
# Function for Formatting the extracted information to the wf13 format
# Input: Dictionary (X) that contains all the information
# Output: A string with fw13 format
#-------------------------------------------------------------------
def FormatFW13( X ):
    Fields = (('W13',(0,'3A')),('Station Number',(3,'6N')),('Ob Date',(9,'8A')),('Ob Time',(17,'4A')),
              ('Type',(21,'1A')),('State of Weather',(22,'1N')),('Temp',(23,'3N')),('Moisture',(26,'3N')),
              ('WindDir',(29,'3N')),('WindSpeed',(32,'3N')),('10hr Fuel',(35,'2N')),('Tmax',(37,'3N')),
              ('Tmin',(40,'3N')),('RHmax',(43,'3N')),('RHmin',(46,'3N')),('PrecipDur',(49,'2N')),
              ('PrecipAmt',(51,'5N')),('WetFlag',(56,'1A')),('Herb',(57,'2N')),('Shrub',(59,'2N')),
              ('MoistType',(61,'1N')),('MeasType',(62,'1N')),('SeasonCode',(63,'1N')),('SolarRad',(64,'4N')),
              ('GustDir',(68,'3N')),('GustSpeed',(71,'3N')),('SnowFlag',(74,'1A')) ##Updated 02/03/2016 for the new three parameters
              )
    Out = []
    for f,p in Fields:
        val = X[f]
        #str(X[f]).zfill()
        length = int(p[1][:-1]) #not working
        format = p[1][-1]
        #This is the total precipitation in the previous 24 hours, given in thousands of an inch. For
        #example, an observation of 0.04? would be entered as ___40, preceded by three
        #blanks/spaces. An observation of 1.25? would be entered as _1250, preceded by one space.
        #An observation of no rainfall would be entered as all blanks/spaces.

        if f=='PrecipAmt':
            if val == 0:
                val=-999
            else:
                val*=1000
        ##Check if there is a zero in WindSpeed and WindDirection and
        WindParaList = ['WindSpeed','WindDir','Temp','Tmax','Tmin','Moisture','RHmax','RHmin','10hr Fuel','GustDir','GustSpeed']
        if f in WindParaList :
            #print 'Val:',val
            if val == 0:
               val=-999
            #print 'Val:',val
        else:
            ZeroPad = ''
        if format == 'N' and val != -999:
            #q = str(0).zfill(length)
            q = '%%%s%dd' % (ZeroPad,length)
        elif format == 'N' and val == -999:
            val = ' '
            q = '%%%s%ds' % (ZeroPad,length)
        else:
            q = '%%%ds' % length
        try:
            Out.append( q % val )
        except:
            print f, p, q, val, type(val)
    #print Out
    return string.join( Out, '' )
#-------------------------------------------------------------------------------
# Function for generate hours report for each hour (including the 'O' and 'R' record)
# Input: data.fram for the day and row for the current hour and a dictionary
# Output: A dictionary X with all required information
#------------------------------------------------------------------------------
def Report9(asos,X,row):
    UTCtime = row['valid']
    LOCTIME=UTC2LOCAL(UTCtime)
    X['Ob Date'] = LOCTIME.strftime("%Y%m%d")
    X['Ob Time']=LOCTIME.strftime("%H%M")
    hour = LOCTIME.strftime("%H")

    X['SeasonCode'] = get_season(LOCTIME)
    ##Larry mentioned to round up the minutes to whole instead of changing the system configuration,
    ##however, Brad and Mike prefer use the original time for human intervention (updated 09/24/2015)
    if hour=='12':
        X['Type']='O'
    else:
        X['Type']='R'
    X['Temp'] = formatFloat(row['tmpf'])
    X['Moisture']= formatFloat(row[' relh'])#use type 2 Relative Humidity
    X['WindSpeed'] = formatFloat(row['WindSpeed']) #this has been re-calculated
    if X['WindSpeed']==0:
        X['WindDir']=0
    else:
        X['WindDir'] = formatFloat(row[' drct'])

    X['Tmax'] = formatFloat( max(asos['tmpf']) )
    X['Tmin'] = formatFloat( min(asos['tmpf']) )
    TotPr = 0
    X['RHmax'] = formatFloat(max(asos[' relh']))
    X['RHmin'] = formatFloat(min(asos[' relh']))
    #This should be a method to compute the measurable precipitation, the measuable precipitation should be bigger than 0.005
    X['PrecipDur'] = asos['precip_duration'].sum()
    TotPr = asos[' p01i'].sum()
    #print 'TotPr', TotPr
    #Determin the SOW value by a defined rule
    #This is the total precipitation in the previous 24 hours, given in thousands of an inch. For
    #example, an observation of 0.04? would be entered as ___40, preceded by three
    #blanks/spaces. An observation of 1.25? would be entered as _1250, preceded by one space.
    #An observation of no rainfall would be entered as all blanks/spaces.
    X['PrecipAmt'] = formatPrecip(TotPr)
    StateOfWeather(X,row)
    #Moisture Type code (1=Wet bulb, 2=Relative Humidity, 3=Dewpoint).
    X['MoistType'] = 2
    #Measurement Type code: 1=U.S.
    X['MeasType'] = 1
    #Solar radiation (watts per square meter).
    X['SolarRad'] = 0 #Need to discuss the default value suppose it to be 0
    #Add the Greeness factor for recent days
    X['Herb'] = herbaceousGreennessF[X['Station Number']]
    X['Shrub'] = shrubGreennessF[X['Station Number']]
    X['SeasonCode'] = seasonCode[X['Station Number']]
    return X

def Report13(asos,X,row):
    UTCtime = row['valid']
    LOCTIME=UTC2LOCAL(UTCtime)
    X['Ob Date'] = LOCTIME.strftime("%Y%m%d")
    X['Ob Time']=LOCTIME.strftime("%H%M")
    hour = LOCTIME.strftime("%H")

    X['SeasonCode'] = get_season(LOCTIME)
    ##Larry mentioned to round up the minutes to whole instead of changing the system configuration,
    ##however, Brad and Mike prefer use the original time for human intervention (updated 09/24/2015)
    if hour=='12':
        X['Type']='O'
    else:
        X['Type']='R'
    X['Temp'] = formatFloat(row['tmpf'])
    X['Moisture']= formatFloat(row[' relh'])#use type 2 Relative Humidity
    #X['WindDir'] = formatFloat(row[' drct'])
    #X['WindSpeed'] = formatFloat(row['WindSpeed']) #this has been re-calculated
    X['WindSpeed'] = formatFloat(row['WindSpeed']) #this has been re-calculated
    if X['WindSpeed']==0:
        X['WindDir']=0
    else:
        X['WindDir'] = formatFloat(row[' drct'])

    X['GustSpeed'] = formatFloat(row['GustSpeed'])
    if X['GustSpeed']==0:
        X['GustDir']= 0
    else:
        X['GustDir'] = X['WindDir']
    #print 'Ob Time:',X['Ob Time'],'WindSpeed:',X['WindSpeed'],'WindDir:',X['WindDir'],'GustDir:',X['GustDir'],'GustSpeed:',X['GustSpeed']

    X['Tmax'] = formatFloat( max(asos['tmpf']) )
    X['Tmin'] = formatFloat( min(asos['tmpf']) )
    TotPr = 0
    X['RHmax'] = formatFloat(max(asos[' relh']))
    X['RHmin'] = formatFloat(min(asos[' relh']))
    #This should be a method to compute the measurable precipitation, the measuable precipitation should be bigger than 0.005
    X['PrecipDur'] = asos['precip_duration'].sum()
    TotPr = asos[' p01i'].sum()
    #print 'TotPr', TotPr
    #Determin the SOW value by a defined rule
    #This is the total precipitation in the previous 24 hours, given in thousands of an inch. For
    #example, an observation of 0.04? would be entered as ___40, preceded by three
    #blanks/spaces. An observation of 1.25? would be entered as _1250, preceded by one space.
    #An observation of no rainfall would be entered as all blanks/spaces.
    X['PrecipAmt'] = formatPrecip(TotPr)
    StateOfWeather(X,row)
    #Moisture Type code (1=Wet bulb, 2=Relative Humidity, 3=Dewpoint).
    X['MoistType'] = 2
    #Measurement Type code: 1=U.S.
    X['MeasType'] = 1
    #Solar radiation (watts per square meter).
    X['SolarRad'] = 0 #Need to discuss the default value suppose it to be 0
    #Add the Greeness factor for recent days
    X['Herb'] = herbaceousGreennessF[X['Station Number']]
    X['Shrub'] = shrubGreennessF[X['Station Number']]
    X['SeasonCode'] = seasonCode[X['Station Number']]
    return X
#----------------------------------------------------------------------------------------
# Function to determin the State of Weather value by a defined rule
# RULE: from 9 to 0
# first look into the raw_text for lightning information,
# then to wx_string for thunderstorm shower, snow ,rain and drizzle
# then to skycover for 4,3,2,1
# Input : Dictionary and Current hour record
# Updated (09/30/2015):
#----------------------------------------------------------------------------------------
def StateOfWeather(X,row):
    rawInput = str(row[' metar'])
    #print type(rawInput), rawInput
    wxstring = str(row[' presentwx'])
    #print type(wxstring), wxstring
    skycover = str(row['sky_cover'])
    #print type(skycover), skycover
    skycover = row['sky_cover']#how does pandas read several field with the same name(probably a new name?)
    if not rawInput.find('LTG') == -1:#changed from DSNT to LTG 09182015
        X['State of Weather'] = 9
    elif not wxstring.find('TS') ==-1 :
        X['State of Weather'] = 9
    elif not wxstring.find('SH') ==-1 :
        X['State of Weather'] = 8
    elif not wxstring.find('SN') ==-1 :
        X['State of Weather'] = 7
    elif not wxstring.find('RA') ==-1 :
        #How to determin the rain code? bigger than 0.1,
        if X['PrecipAmt'] >= 0.1:
            X['State of Weather'] = 6
    elif not wxstring.find('DZ') ==-1 :
        #How to determin the drizzle code? bigger than 0.01
        if X['PrecipAmt'] >= 0.01:
            X['State of Weather'] = 5
    elif not wxstring.find('FG') ==-1 :
        X['State of Weather'] = 4
    #elif not wxstring.find('HZ') ==-1 :
    #    X['State of Weather'] = 4
    elif not wxstring.find('BR') ==-1 :
        X['State of Weather'] = 4
    elif 'OVC' in skycover:
        X['State of Weather'] = 3
    elif 'BKN' in skycover:
        X['State of Weather'] = 2
    elif 'SCT' in skycover or 'FEW' in skycover:
        X['State of Weather'] = 1
    elif 'CLR' in skycover or 'SKC' in skycover:
        X['State of Weather'] = 0
    #######################################################
    #Updated 09-28-2015
    #Per Discussion with Mike and Brad on 09/24/2015
    #The wet flag will be always be set to 'N' because human intervention will be needed for the determination
    #Updated on 10/30/2015, Larry suggested when State of Weather is 5,6 or 7, the We flag should be set to 'Y'
    if X['State of Weather'] == 5 or X['State of Weather'] == 6 or X['State of Weather'] == 7:
        X['WetFlag']= 'Y'
    else:
        X['WetFlag']= 'N'
##    #If the SOW is 8 (showers) or 9 (thunderstorms) and the station of interest reported any precipitation in the past hour, set the Wet Flag to Y.
##    elif X['State of Weather'] == 8 or X['State of Weather'] == 9:
##        if row['precip_duration'] == 1:
##            X['WetFlag']= 'Y'
##    else:
##        X['WetFlag']= 'N'
    return X

#-----------------------------------------------------------------------------------------------
# Function to inteprete the downloaded ASOS historical file and extract the relevant information
# Input : txt file downloaded from IEM archive
# Output: string stream fils formatted in fw13
#-----------------------------------------------------------------------------------------------
"""##################################################
reference from http://mesonet.agron.iastate.edu/request/download.phtml
Downloaded Variable Description:
station: three or four character site identifier
valid:    timestamp of the observation
tmpf:    Air Temperature in Fahrenheit, typically @ 2 meters
dwpf:    Dew Point Temperature in Fahrenheit, typically @ 2 meters
relh:    Relative Humidity in %
drct:    Wind Direction in degrees from north
sknt:    Wind Speed in knots
p01i:    One hour precipitation for the period from the observation time to the time of the previous hourly precipitation reset.
         This varies slightly by site. Values are in inches. This value may or may not contain frozen precipitation melted by some device on the sensor or
         estimated by some other means. Unfortunately, we do not know of an authoritative database denoting which station has which sensor.
vsby:    Visibility in miles
gust:    Wind Gust in knots
skyc1:    Sky Level 1 Coverage
skyc2:    Sky Level 2 Coverage
skyc3:    Sky Level 3 Coverage
skyc4:    Sky Level 4 Coverage
presentwx:    Present Weather Codes (space seperated)
metar:    unprocessed reported observation in METAR format
NOTE-10-09-2015: M and space means no data
##########################################################"""
#Be care the space on the field name!
#----------------------------------------------------------------------------------------
def IntepreteHistASOS(histASOS,STATION,ID):
#def IntepreteHistASOS(STATION,ID):
    print 'Processing Historical ASOS for %s : %s' % (STATION, ID)
    #histASOS = "c:\\DEV\\ASOS\\ASOS_21\\" + outfn
    majoritylst = []
    with open(histASOS) as filt_csv:
        pread = pandas.read_csv(filt_csv,skiprows=5, low_memory=False)
        asos = pread [['station',
                     'valid',
                     'tmpf',
                     ' dwpf',
                     ' relh',
                     ' drct',
                     ' sknt',
                     ' p01i',
                     ' gust',
                     ' skyc1',
                     ' skyc2',
                     ' skyc3',
                     ' presentwx',
                     ' metar']]

        #Remove duplicated records suggested by Larry 10-14-2015
        asos = asos.drop_duplicates()

        #Add a colum based on the observing minutes
        asos['minute'] = asos.loc[:,('valid')].str[14:16]

        #2 do a statistical on them and get the flag time(a majority time)
        #for record in asos['minute']:
        for record in asos.loc[:,('minute')]:
            majoritylst.append(record)
        flagtime = max(set(majoritylst),key=majoritylst.count)

        #3 tiss out the hour-in-between records and this is the observation data that is gonna to use
        asos = asos[asos['minute']==flagtime]

        #Updated 10-28-2015 still found duplicated records with the same time only difference is where the metar field contains 'METAR'
        asos = asos.drop_duplicates(['valid'],  keep='last')

        #Fill the no data value with blank for the four levels of sky cover and wx_string
        asos[' skyc1'].fillna('', inplace=True)
        asos[' skyc2'].fillna('', inplace=True)
        asos[' skyc3'].fillna('', inplace=True)
        asos[' presentwx'].fillna('', inplace=True)

        #Merge three field together
        asos['sky_cover'] = asos.apply(SKY,axis=1)

        #Fill the 'M' value with 0
        asos[[' sknt']] = asos[[' sknt']].replace('M',0).astype(float)
        asos[[' drct']] = asos[[' drct']].replace('M',0).astype(float)
        asos[[' p01i']] = asos[[' p01i']].replace('M',0).astype(float)
        asos[[' relh']] = asos[[' relh']].replace('M',0).astype(float)
        asos[[' gust']] = asos[[' gust']].replace('M',0).astype(float)

        #Fill the 'M' value with NULL
        asos[[' presentwx']] = asos[[' presentwx']].replace('M','').astype(str)
        asos[['tmpf']] = asos[['tmpf']].replace('M',0).astype(float)
        #Wind speed at 6m, only pick up the 13 hours wind speed!!
        #Wind speed at 6m, only pick up the 13 hours wind speed!!
        asos['WindSpeed']=asos.ix[:,' sknt'].apply(windspeed)
        #for gust updated on 02/15/2016
        asos['GustSpeed']=asos.ix[:,' gust'].apply(windspeed)
        #To define the Precipitation Duration hours
        asos['precip_duration']=asos.ix[:,' p01i'].apply(precipDuration)

        #define a dictionary to hold all the information
        ##Fire Weather 9 Format
        X9 = {'W98':'W98', 'Station Number':'000000', 'Ob Date':'YYYYMMDD', 'Ob Time':0,
                  'Type':'R', 'State of Weather':0, 'Temp':0, 'Moisture':0,
                  'WindDir':0, 'WindSpeed':0, '10hr Fuel':0, 'Tmax':-999,
                  'Tmin':-999, 'RHmax':-999, 'RHmin':-999, 'PrecipDur':0,
                  'PrecipAmt':0, 'WetFlag':'N', 'Herb':-999, 'Shrub':-999,
                  'MoistType':2, 'MeasType':1, 'SeasonCode':0, 'SolarRad':0
                }
        ##Fire Weather 13 Format
        X13 = {'W13':'W13', 'Station Number':'000000', 'Ob Date':'YYYYMMDD', 'Ob Time':0,
          'Type':'R', 'State of Weather':0, 'Temp':0, 'Moisture':0,
          'WindDir':0, 'WindSpeed':0, '10hr Fuel':0, 'Tmax':-999,
          'Tmin':-999, 'RHmax':-999, 'RHmin':-999, 'PrecipDur':0,
          'PrecipAmt':0, 'WetFlag':'N', 'Herb':20, 'Shrub':15,
          'MoistType':2, 'MeasType':1, 'SeasonCode':3, 'SolarRad':0,
          'GustDir':0,'GustSpeed':0,'SnowFlag':'N' ##Updated 02/03/2016 for the new three parameters
          ##According to Juan, the gust direction of the peak wind should be the same with the hourly wind direction
        }

        #pass the station ID
        X9['Station Number'] = ID
        X13['Station Number'] = ID


        #Create a file with station name ('tx-asos-KFST.fw9')
        #os.path.join(WorkSpace, "HIST")
        #fileWF9 = WorkSpace + '\\tx-asos-K' + STATION + '.fw9'
        fileWF13 = HISTFW13 + '\\tx-asos-K' + STATION + '.fw13'
        #fileWF9 = WorkSpace + '\\tx-asos.fw9'

        #with open(fileWF9,'a') as F9, open(fileWF13,'a') as F13:
        with open(fileWF13,'a') as F13:
        #updated 10-13-2015, the hour should be started 24 hours after the first day and there will more than 24 records
            for hour in range(23,len(asos.index),1):

                df = asos[hour-23:hour+1]
                #The data frame should be 24 records long. other wise quit
                if len(df.index) < 24:
                    print "there are not enough records for the 24 hour analyses!"
                    return
                else:
                    #should be a way to subset the dataframe by loop from the first row until the previous 24 hour
                    #Detect missing record to avoid 2 'o' report for a station
                    checkMissingRecords(df)
                    #Transform the current row into a dictionary for better operation
                    #currenthour= df[:-1].set_index('station').T.to_dict()
                    currenthour = df.tail(1).set_index('station').T.to_dict()[STATION]#Note: this is different from the Aviation data!Use the last record!
                    ##Pass the previous 24 hours and current hour record for WIMS fw9 and fw13 format
##                    Report9(df,X9,currenthour)
##                    F9.write( FormatFW9( X9 ) +'\n' )
                    Report13(df,X13,currenthour)
                    F13.write( FormatFW13( X13 ) +'\n' )

# Define primary workspace based on location of script
WorkSpace = os.getcwd()
#The downloaded file and processed file will be in the HIST directory
HISTWX = os.path.join(WorkSpace, "HIST")
HISTFW13 = os.path.join(HISTWX,'FW13')

if not os.path.exists(HISTWX):
    os.makedirs(HISTWX)

if not os.path.exists(HISTFW13):
    os.makedirs(HISTFW13)

#List of ASOS stations used for Texas
Stations = {"KDHT": 418702,
            "KAMA": 418803,
            "KSPS": 419302,
            "KINK": 417501,
            "KFST": 417601,
            "KLBB": 419002,
            "KJCT": 417803,
            "KSJT": 419204,
            "KELP": 416901,
            "KDRT": 418003,
            "KHDO": 418103,
            "KSSF": 418104,
            "KCOT": 418402,
            "KALI": 418504,
            "KBAZ": 418105,
            "KCLL": 413901,
            "KCRS": 412001,
            "KTYR": 411701,
            "KTRL": 419703,
            "KDTO": 419603,
            "KMWL": 419404}

#Greeness code dictionary Should be derived from a csv file that modify by Mike
GreenessCodeCSV = os.path.join(WorkSpace,"ASOS_SeasonCode_GreenessCode.csv")
    # create groups and sorted stations lists for later use
shrubGreennessF= {}
herbaceousGreennessF = {}
seasonCode = {}
# read new csv file
with open(GreenessCodeCSV,mode='r') as rawcsv:
    rawReader = csv.reader(rawcsv, delimiter = ',')
    rawReader.next()
    for row in rawReader:
        #print row['WIMS ID']
        shrubGreennessF[int(row[6])]=int(row[7])
        herbaceousGreennessF[int(row[6])]=int(row[8])
        seasonCode[int(row[6])]=int(row[9])

#Define a start and end date for historical data (UTC time will be applied)
starts = datetime(2016, 2, 22)
endts = datetime(2016, 2, 25)

#Downloading historical ASOS records from Iowa MesoNet
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
SERVICE += "data=all&tz=Etc/UTC&format=comma&latlon=yes&"

SERVICE += starts.strftime('year1=%Y&month1=%m&day1=%d&')
SERVICE += endts.strftime('year2=%Y&month2=%m&day2=%d&')

#network here used for texas ASOS
network = 'TX_ASOS'
uri = "http://mesonet.agron.iastate.edu/geojson/network.php?network=%s" % (
                                                                    network,)
#Download and Process the historical asos records
for STATION,ID in Stations.items():
    station = STATION[1:]
    uri = '%s&station=%s' % (SERVICE, station)
    print 'Network: %s Downloading: %s' % (network, station)
    outfn = HISTWX + '\\%s_%s_%s.txt' % (station, starts.strftime("%Y%m%d%H%M"),endts.strftime("%Y%m%d%H%M"))

    try:
        #Download the historical asos for each station
        urllib.urlretrieve(uri,outfn)
        #Parse the information into FW13 format
        IntepreteHistASOS(outfn,station,ID)

    except:
        MSG = "The ASOS data were not dowloaded successfully for Station: %s"% (station)
        print "Unexpected error:", sys.exc_info()[0]
