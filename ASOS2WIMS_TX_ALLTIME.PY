#-------------------------------------------------------------------------------
# Name:      ASOS2WIMS_TX.py
# Purpose:   Compile ASOS weather into FW9 for feeding WIMS
# Author:    Peter Yang
# Note : 3 Places need to change when daylight saving time change!RemodeRecords function(1)Report9 function(2)after "hoursBeforeNow = 48" (3)
# Created:   01/10/2015
# Updated: 03/07/2017 Re-enabled the email notification
# Updated: 03/07/2017 Fix the day saving time issue
# Updated: 07/19/2017 Fix the issue with daylight saving again
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
import logging
#from datetime import date, datetime, timedelta
import datetime
from dateutil import tz
import smtplib
pandas.options.mode.chained_assignment = None  # default='warn'

#Send email if issue happenned for notifying Mike for mannually editting
def sendEmail(TXT):
    server = smtplib.SMTP('tfsbarracuda.tamu.edu', 25)
    #server.set_debuglevel(1)
    SUBJECT = 'There is an issue with ASOS4WIMS'
    message = 'Subject: %s\n\n%s' % (SUBJECT, TXT)
    print "Sending email to " + message
    tolist=["pyang@tfs.tamu.edu","mdunivan@tfs.tamu.edu"]
    server.sendmail("pyang@tfs.tamu.edu", tolist, message)

#For the season code:input an actual day (today) to get the season code
#This should be different for each station (green up time?)
##Y = 2000 # dummy leap year to allow input X-02-29 (leap day)
##seasons = [(1, (date(Y,  1,  1),  date(Y,  3, 20))),
##           (2, (date(Y,  3, 21),  date(Y,  6, 20))),
##           (3, (date(Y,  6, 21),  date(Y,  9, 22))),
##           (4, (date(Y,  9, 23),  date(Y, 12, 20))),
##           (1, (date(Y, 12, 21),  date(Y, 12, 31)))]

def get_season(now):
    if isinstance(now, datetime):
        now = now.date()
    now = now.replace(year=Y)
    return next(season for season, (start, end) in seasons
                if start <= now <= end)
#-----------------------------------------------------------------------------------------
# Convert UTC to Central Zone (can be any zone)
#-----------------------------------------------------------------------------------------
def UTC2LOCAL(TIMESTR):
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal() #changed to local zone
    #to_zone = tz.gettz("US/Central")
    utc= datetime.strptime(TIMESTR,"%Y-%m-%dT%H:%M:%SZ")
    utc=utc.replace(tzinfo=from_zone)
    local = utc.astimezone(to_zone)
    return local

#--------------------------------------------------------------------------------------------------------------------------------
# Convert UTC to Standard time (only for central standard time)
# Since we know there will be always 6 hours difference between the UTC and standard time(disregard the daylight saving effect
#-------------------------------------------------------------------------------------------------------------------------------
def UTC2STANDARD(TIMESTR):
    utc= datetime.datetime.strptime(TIMESTR,"%Y-%m-%dT%H:%M:%SZ")
    standard = utc - datetime.timedelta(hours=6)
    return standard

#------------------------------------------------------------
# A function to round the late 50 minutes to a full hour
#------------------------------------------------------------
def RoundHour(local):
    Hour = local.hour
    Minute = local.minute
    if Minute//50 >= 1:
        Hour = Hour + 1
    return Hour

#Fahrenheit to Celsius convert
def F2C( T ):
    return (T-32.)*5./9.

def C2F( T ):
    return T * 9./5. + 32.

def SatVapPres( T ):
    return math.exp( (16.78 * T - 116.9 )/( T + 237.3 ) )

#Formatting Float into Int
def formatFloat(v):
    return int(round(v))

#Formatting precipitation with 2 decimals
def formatPrecip(p):
    return round(p, 2)

#----------------------------------------------------------------------------
#RH: =100*(EXP((17.625*TD)/(243.04+TD))/EXP((17.625*T)/(243.04+T)))
#T = Temperature in Celcius TD = Dewpoint in Celcius Raw ASOS data in Celcius
#Calculate Relative Humidity
#----------------------------------------------------------------------------
def RH(T,TD):
    return 100*(math.exp((17.625*TD)/(243.04+TD))/math.exp((17.625*T)/(243.04+T)))

def RH(series):
    return 100*(math.exp((17.625* series['dewpoint_c'])/(243.04 + series['dewpoint_c']))/math.exp((17.625*series['temp_c'])/(243.04+series['temp_c'])))

#-----------------------------------------------------------------------------------------
# Processing the 4 level of sky cover into one column
def SKY(series):
    return series['sky_cover'] + series['sky_cover.1'] + series['sky_cover.2'] + series['sky_cover.3']


#-----------------------------------------------------------------------------------------
# Processing the UTC time to Loca;
def UTC4LOCAL(observation_time):
    #print observation_time
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal() #changed to local zone
    #to_zone = tz.gettz("US/Central")
    utc= datetime.datetime.strptime(observation_time,"%Y-%m-%dT%H:%M:%SZ")
    utc=utc.replace(tzinfo=from_zone)
    local = utc.astimezone(to_zone)
    #print local
    return local

#---------------------------------------------------------------------------
#Function to remove the post-13 hour observation
#---------------------------------------------------------------------------
def RemoveLatestRecords(df):
    #time13  =
    print 'lengthbefore:',len(df)
    #This could not just bigger than 13 hours because there are yesterday's records
    #First get the flag minutes and today's 13 hours string like 12:53, how can I get it??

    #get the first record to see if it is bigger than 13 (only bigger than 13 records will be removed) but be careful should not remove yesterday's records
    flagminute = df.iloc[0]['minute']
    #change today's thirteen hour into a time object
    #May need a while loop to delete the first record always
    thirteenhour = df.iloc[0]['obs_time_local']
    str_thirteenhour = str(thirteenhour)
    #str_thirteenhour[1][0:1]= '10'
    currentday = str_thirteenhour[:10]
    ##thirteenhour = ' 12'
    #when there is daylight saving time it should be 13 (need to change other as well)
    #Updated on 11/05/2016 change the thirteen hour to 12
    #Updated on 03/12/2017 change the thirteen hour to 13
    thirteenhour = ' 13'
    remaining = str_thirteenhour[13:]
    flagtime = currentday + thirteenhour + remaining
    #Timestamp('flagtime')
    thirteenhourtimestamp = pandas.Timestamp(flagtime)#,tz='utc')
    #print 'flagtime:',flagtime,type(thirteenhour),thirteenhourtimestamp,type(thirteenhourtimestamp)
    #Compare each row with the thirteenhourtimestampe,
    for index,row in df.iterrows():
        HourLatest = row['obs_time_local']
        #HourLatest   = datetime.strptime(row['observation_time'],"%Y-%m-%dT%H:%M:%SZ")
        #tdelta =  HourLatest - thirteenhourtimestamp
        #print HourLatest,type(HourLatest),thirteenhourtimestamp,type(thirteenhourtimestamp)
        if HourLatest > thirteenhourtimestamp:
            #print 'remove!'
            df.drop(index,inplace=True)
##        if(tdelta.days>= 1):
##            df.drop(index,inplace=True)
##            #Need to subset the records when there are hours after 13:00 hours
##        #timeBegin = datetime.strptime(asos.iloc[0]['obs_time_local'],"%Y-%m-%dT%H:%M:%SZ")
##        asos.reindex()
##        print
##        HourLatest = asos.iloc[0]['obs_time_local'].strftime("%H")
##        HourToRemove = int(HourLatest) - 12
##        print HourToRemove
        #asos = asos.iloc[HourToRemove,24+HourToRemove]
    df.reset_index(inplace = True)
    print 'lengthafter:',len(df)
    #df.reset_index(drop=True) ??not working!
    return df
#-----------------------------------------------------------------------------------------------------------------
# ASOS wind sensors are at a height of 10 meters, but the RAWS/WIMS standard is for 6 meter/20 foot winds.
#To estimate the 6 meter wind speed from the 10 meter measurement, the logarithmic wind profile method  math.log(6/0.0984)/math.log(10/0.0984) can be used
#To convert the knot to mph, the 1.15078 ration is used. Mike suggest mannually reduce the windspeed by 10% (*0.9) for WIMS
#------------------------------------------------------------------------------------------------------------------
def windspeed(wind_speed_kt):
    return wind_speed_kt * 0.9 * math.log(6/0.0984)/math.log(10/0.0984) * 1.15078
    #updated on 10-13-2015: Mike requet to reduce wind 10% down from Larry's suggestion
    #12/10/2015 a dispute will be here if using the *1.15 (to miles) will definately increase the value instead reducing it
    #return gust * 0.90 * math.log(6/0.0984)/math.log(10/0.0984)

#Use a special treatment for the value at 0.005 or smaller precipitation
def CorrectPrcpAmount(p):
    if p <= 0.005 : p = 0.0
    return p

#Define hourly precipitation based on the measurable amount of precipitation
def precipDuration(preci_in):
    if preci_in > 0:
        return 1
    else:
        return 0
#---------------------------------------------------------------------------
#Function to remove the pre-24 hour observation if there are missing records
#To remove the records where the temperature and relative humudity with no-data values
#---------------------------------------------------------------------------
def checkValidRecords(df):
    #check time difference for each hour is 1 hour and for the first and last record is 24
    #can not use now because it maybe the hour after 13:00 hour!!!
    #change today's thirteen hour into a time object
    flagminute = df.iloc[0]['minute']
    thirteenhour = df.iloc[0]['obs_time_local']
    str_thirteenhour = str(thirteenhour)
    currentday = str_thirteenhour[:10]
    thirteenhour = ' 12'
    remaining = str_thirteenhour[13:]
    flagtime = currentday + thirteenhour + remaining
    thirteenhourtimestamp = pandas.Timestamp(flagtime,tz='utc')
    #timeNow = datetime.now(tz.tzutc())
    timeBegin = datetime.datetime.strptime(df.iloc[0]['observation_time'],"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=tz.tzutc())
    tdelta = thirteenhourtimestamp - timeBegin
    days, seconds = tdelta.days, tdelta.seconds
    hours = days * 24 + seconds//3600
    if hours >= 1:
        recordsToRemove = 24 #All the records will not be processed
        #print "The observation data is not updated for Station: ",(row['station_id'])
        #logging.info("The observation data is not updated for Station: %s",row['station_id'])
    else:
        recordsToRemove = 0
        for index,row in df.iterrows():
            timeEnd   = datetime.datetime.strptime(row['observation_time'],"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=tz.tzutc())
            tdelta =  timeBegin - timeEnd
            if tdelta.days >= 1:
                recordsToRemove = recordsToRemove + 1
                print "There is an unvalid record for Station: ",(row['station_id'])
                logging.info("There is an unvalid record for Station: %s %s",row['station_id'],timeEnd)

    return recordsToRemove
#---------------------------------------------------------------------------
#Function to remove the pre-24 hour observation if there are missing records
#---------------------------------------------------------------------------
def checkMissingRecords(df):
    #check time difference for each hour is 1 hour and for the first and last record is 24
    df.dropna(axis=0, how='any', inplace=True)
    df.reset_index(inplace = True)
    #print df
    if len(df) >0 :
        timeBegin = datetime.datetime.strptime(df.iloc[0]['observation_time'],"%Y-%m-%dT%H:%M:%SZ")
        for index,row in df.iterrows():
            timeEnd   = datetime.datetime.strptime(row['observation_time'],"%Y-%m-%dT%H:%M:%SZ")
            tdelta =  timeBegin - timeEnd
            if(tdelta.days>= 1):
                df.drop(index,inplace=True)
                #print "There is a missing record for Station: ",(row['station_id'])
                logging.info("There is a missing record for Station: %s",row['station_id'])
    return df

#--------------------------------------------------------------------
# Function for Formatting the extracted information to the wf9 format
# Input: Dictionary (X) that contains all the information
# Output: A string with fw9 format
#-------------------------------------------------------------------
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
        WindParaList = ['WindSpeed','WindDir']
        WindParaList = ['WindSpeed']#Updated on 01092017 after meeting discussion that the 0 should be replaced by 3 mph
        if f in WindParaList :
            if val == 0:
               val = 3 #Updated on 01092017 after discussion that the 0 windspeed should be replaced by 3 mph
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
        WindParaList = ['WindSpeed','WindDir','GustDir','GustSpeed']
        if f in WindParaList :
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

#-------------------------------------------------------------------------------
# Function for generate hours report for each hour (including the 'O' and 'R' record)
# Input: data.fram for the day and row for the current hour and a dictionary
# Output: A dictionary X with all required information
#------------------------------------------------------------------------------
def Report9(asos,X,row):
    UTCtime = row['observation_time']
    ##local time should be standard time
    STANDARDTIME=UTC2STANDARD(UTCtime)
    X['Ob Date'] = STANDARDTIME.strftime("%Y%m%d")
    X['Ob Time']=STANDARDTIME.strftime("%H%M")
    #X['SeasonCode'] = get_season(LOCTIME)
    hour = STANDARDTIME.strftime("%H")
    ##Larry mentioned to round up the minutes to whole instead of changing the system configuration,
    ##however, Brad and Mike prefer use the original time for human intervention (updated 09/24/2015)
    ##A descrepency happened with the WIMS where Mike doesn't wnat to configure the station catalogy twice a year
    ##if hour=='13': #updated on 03122017 for daily saving time
    ##Requested by Mike on 09/21/2017 to put all records with "R"
##    if hour=='12':##updated on 07192017 for resoving the wims issue with 13:00 observation timestamp
##        X['Type']='O'
##    else:
##        X['Type']='R'
    X['Temp'] = formatFloat(C2F(row['temp_c']))
    X['Moisture']= formatFloat(row['RH'])#use type 2 Relative Humidity

    X['WindSpeed'] = formatFloat(row['WindSpeed']) #this has been re-calculated
    if X['WindSpeed']==0:
        X['WindDir']=0
    else:
        X['WindDir'] = formatFloat(row['wind_dir_degrees'])

##    X['GustSpeed'] = formatFloat(row['GustSpeed'])
##    if X['GustSpeed']==0:
##        X['GustDir']= 0
##    else:
##        X['GustDir'] = X['WindDir']

    #print 'Ob Time:',X['Ob Time'],'WindSpeed:',X['WindSpeed'],'WindDir:',X['WindDir']##'GustDir:',X['GustDir'],'GustSpeed:',X['GustSpeed']
    X['Tmax'] = formatFloat(C2F( max(asos['temp_c']) ))
    X['Tmin'] = formatFloat(C2F( min(asos['temp_c']) ))
    TotPr = 0
    X['RHmax'] = formatFloat(max(asos['RH']))
    X['RHmin'] = formatFloat(min(asos['RH']))
    #This should be a method to compute the measurable precipitation, the measuable precipitation should be bigger than 0.005
    X['PrecipDur'] = asos['precip_duration'].sum()
    TotPr = asos['precip_in'].sum()
    #print 'TotPr', TotPr
    #Determin the SOW value by a defined rule
    #This is the total precipitation in the previous 24 hours, given in thousands of an inch. For
    #example, an observation of 0.04? would be entered as ___40, preceded by three
    #blanks/spaces. An observation of 1.25? would be entered as _1250, preceded by one space.
    #An observation of no rainfall would be entered as all blanks/spaces.
    #Updated 10/26/2015, rounding precip into hundredths
    X['PrecipAmt'] = formatPrecip(TotPr)
    StateOfWeather(X,row)
    #Moisture Type code (1=Wet bulb, 2=Relative Humidity, 3=Dewpoint).
    X['MoistType'] = 2
    #Measurement Type code: 1=U.S.
    #X['MeasType'] = 1
    #Solar radiation (watts per square meter).
    X['SolarRad'] = 0 #Need to discuss the default value suppose it to be 0

    X['Herb'] = herbaceousGreennessF[X['Station Number']]
    X['Shrub'] = shrubGreennessF[X['Station Number']]
    X['SeasonCode'] = seasonCode[X['Station Number']]

##    if X['State of Weather'] == 7:
##        X[SnowFlag]='Y'
##    print X
    return X

def Report13(asos,X,row):
    UTCtime = row['observation_time']
    ##local time should be standard time
    STANDARDTIME=UTC2STANDARD(UTCtime)
    X['Ob Date'] = STANDARDTIME.strftime("%Y%m%d")
    X['Ob Time']=STANDARDTIME.strftime("%H%M")
    #X['SeasonCode'] = get_season(LOCTIME)
    hour = STANDARDTIME.strftime("%H")
    ##Larry mentioned to round up the minutes to whole instead of changing the system configuration,
    ##however, Brad and Mike prefer use the original time for human intervention (updated 09/24/2015)
    ##A descrepency happened with the WIMS where Mike doesn't wnat to configure the station catalogy twice a year
    ##if hour=='13': #updated on 03122017 for daily saving time
##    if hour=='12':##updated on 07192017 for resoving the wims issue with 13:00 observation timestamp
##        X['Type']='O'
##    else:
##        X['Type']='R'

    X['Temp'] = formatFloat(C2F(row['temp_c']))
    X['Moisture']= formatFloat(row['RH'])#use type 2 Relative Humidity

    X['WindSpeed'] = formatFloat(row['WindSpeed']) #this has been re-calculated
    if X['WindSpeed']==0:
        X['WindDir']=0
    else:
        X['WindDir'] = formatFloat(row['wind_dir_degrees'])

    X['GustSpeed'] = formatFloat(row['GustSpeed'])
    if X['GustSpeed']==0:
        X['GustDir']= 0
    else:
        X['GustDir'] = X['WindDir']

    ##print 'Ob Time:',X['Ob Time'],'WindSpeed:',X['WindSpeed'],'WindDir:',X['WindDir'],'GustDir:',X['GustDir'],'GustSpeed:',X['GustSpeed']
    X['Tmax'] = formatFloat(C2F( max(asos['temp_c']) ))
    X['Tmin'] = formatFloat(C2F( min(asos['temp_c']) ))
    TotPr = 0
    X['RHmax'] = formatFloat(max(asos['RH']))
    X['RHmin'] = formatFloat(min(asos['RH']))
    #This should be a method to compute the measurable precipitation, the measuable precipitation should be bigger than 0.005
    X['PrecipDur'] = asos['precip_duration'].sum()
    TotPr = asos['precip_in'].sum()
    #print 'TotPr', TotPr
    #Determin the SOW value by a defined rule
    #This is the total precipitation in the previous 24 hours, given in thousands of an inch. For
    #example, an observation of 0.04? would be entered as ___40, preceded by three
    #blanks/spaces. An observation of 1.25? would be entered as _1250, preceded by one space.
    #An observation of no rainfall would be entered as all blanks/spaces.
    #Updated 10/26/2015, rounding precip into hundredths
    X['PrecipAmt'] = formatPrecip(TotPr)
    StateOfWeather(X,row)
    #Moisture Type code (1=Wet bulb, 2=Relative Humidity, 3=Dewpoint).
    X['MoistType'] = 2
    #Measurement Type code: 1=U.S.
    #X['MeasType'] = 1
    #Solar radiation (watts per square meter).
    X['SolarRad'] = 0 #Need to discuss the default value suppose it to be 0

    X['Herb'] = herbaceousGreennessF[X['Station Number']]
    X['Shrub'] = shrubGreennessF[X['Station Number']]
    X['SeasonCode'] = seasonCode[X['Station Number']]

    if X['State of Weather'] == 7:
        X['SnowFlag']='Y'

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
    rawInput = str(row['raw_text'])
    wxstring = str(row['wx_string'])
    skycover = str(row['sky_cover'])
    skycover = row['sky_cover']
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
    #print 'State of Weather is',X['State of Weather']
    #######################################################
    #Updated 09-28-2015
    #Per Discussion with Mike and Brad on 09/24/2015
    #The wet flag will be always be set to 'N' because human intervention will be needed for the determination
##    if X['State of Weather'] == 5 or X['State of Weather'] == 6 or X['State of Weather'] == 7:
##        X['WetFlag']= 'Y'
##    #If the SOW is 8 (showers) or 9 (thunderstorms) and the station of interest reported any precipitation in the past hour, set the Wet Flag to Y.
##    elif X['State of Weather'] == 8 or X['State of Weather'] == 9:
##        if row['precip_duration'] == 1:
##            X['WetFlag']= 'Y'
##    else:
##        X['WetFlag']= 'N'
    return X

#----------------------------------------------------------------------------------------
# Function to inteprete the downloaded ASOS csv file and extract the relevant information
# Input : csv file for the precious 48 hours;station name
# Output: string stream fils formatted in fw9
#----------------------------------------------------------------------------------------
def IntepreteASOS(csvfile,STATION,ID):
    majoritylst = []
    #Open the downloaded csv for information
    with open(csvfile) as filt_csv:
        pread = pandas.read_csv(filt_csv,skiprows=5)
        #1 Extracting the fields required
        asos = pread[['station_id',
                      'observation_time',
                      'temp_c',
                      'dewpoint_c',
                      'wind_dir_degrees',
                      'wind_speed_kt',
                      'wind_gust_kt',
                      'sky_cover','sky_cover.1','sky_cover.2','sky_cover.3', #Four levels of skycover in the report
                      'precip_in',
                      'raw_text',
                      'wx_string']] #Added raw_txt and wx_string two more fields for SOW detemining

        #Remove duplicated records suggested by Larry 10-14-2015
        asos = asos.drop_duplicates()

        #Add a colum based on the observing minutes
        asos.loc[:,'minute'] = asos.loc[:,('observation_time')].str[14:16]


        #2 do a statistical on them and get the flag time(a majority time)
        #for record in asos['minute']:
        for record in asos.loc[:,('minute')]:
            majoritylst.append(record)
        flagtime = max(set(majoritylst),key=majoritylst.count)

        #3 tiss out the hour-in-between records and this is the observation data that is gonna to use
        asos = asos[asos['minute']==flagtime]

        #Add a column for the local time zone
        asos.loc[:,'obs_time_local']=asos['observation_time'].apply(UTC4LOCAL)


        logging.info("There are total %s Records for Station: %s",len(asos.index),STATION)
        #check how many recodrs for the major time, if the records == 24 go with it
        #if the record less than 24 need to recheck and get. Make sure there are 24 records, otherwise report
        #In order to get a 24 previous records, now the 48 hours record were pulled in
        #print len(asos)
        if len(asos.index) > 48:
            #Which means that there are records after the 13 observation hour, need to remove
            print 'before filter records:',len(asos)
            asos=RemoveLatestRecords(asos)
            print 'after removed records:',len(asos)

        elif len(asos.index) < 48:
            logging.info("There are missing %s Records for Station: %s",48-len(asos.index),STATION)
            #By the time tested, the number is always the same if scheduled run from 13:00 to 13:50, the minutes between 50 and 00 will have missing record
            #Handle the previous where some report were deliberatly changed by the end of the hour. e.g. from 51 to 53 or other time need to recreate the data frame again
        else:
            print 'There are total', len(asos.index),' Records for Station: ', STATION


        #process the hourly report for just the past 24 hours, be careful about the timezone difference

        #Make sure there are 24 records passed onto the Report, using a loop for processing each hour (from the recent to latest 24 hour)

        #Fill the no data value with blank for the four levels of sky cover and wx_string
        asos['sky_cover.1'].fillna('', inplace=True)
        asos['sky_cover.2'].fillna('', inplace=True)
        asos['sky_cover.3'].fillna('', inplace=True)
        asos['wx_string'].fillna('', inplace=True)

        #Change the data type to str instead of float(default by pandas)
        asos[['sky_cover', 'sky_cover.1','sky_cover.2','sky_cover.3','wx_string']] = asos[['sky_cover', 'sky_cover.1','sky_cover.2','sky_cover.3','wx_string']].astype(str)
        #Merge four field together
        asos['sky_cover'] = asos.apply(SKY,axis=1)

        #need to Fill the NUMERICAL data value with 0 for all the numerical fields
        asos['wind_dir_degrees'].fillna(0, inplace=True)
        asos['wind_speed_kt'].fillna(0, inplace=True)
        asos['wind_gust_kt'].fillna(0,inplace=True)
        ###Need to leave it blank if detected as 0

        #4 Fill the No data value with 0 for precipitation
        asos['precip_in'].fillna(0, inplace=True)

        asos = asos[['station_id',
                      'observation_time',
                      'temp_c',
                      'dewpoint_c',
                      'wind_dir_degrees',
                      'wind_speed_kt',
                      'wind_gust_kt',
                      'sky_cover',
                      'precip_in',
                      'raw_text',
                      'wx_string',
                      'minute',
                      'obs_time_local']]

        #define a dictionary to hold all the information
        X9 = {'W98':'W98', 'Station Number':'000000', 'Ob Date':'YYYYMMDD', 'Ob Time':0,
                  'Type':'R', 'State of Weather':0, 'Temp':0, 'Moisture':0,
                  'WindDir':0, 'WindSpeed':0, '10hr Fuel':0, 'Tmax':-999,
                  'Tmin':-999, 'RHmax':-999, 'RHmin':-999, 'PrecipDur':0,
                  'PrecipAmt':0, 'WetFlag':'N', 'Herb':20, 'Shrub':15,
                  'MoistType':2, 'MeasType':1, 'SeasonCode':3, 'SolarRad':0
                }

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
        #Create a file with fixed name ('tx-asos.fw9') suggested by Larry
        with open(fileWF13,'a') as F13, open(fileWF9,'a') as F9:
            #Slicing the dataframe from 48 to 24 for processing
            #the size of the dataframe may change because of valid time (not beyond 24 hours)
            #Initially 23, however, any records beyond 1 day away will be removed to avoid 2 'O' in one day
            #updated 10-20-2015 found there is no today's data at all,need to quit this function
            #Need to subset the records when there are hours after 13:00 hours
            #get the current time for the first record
            df = asos.iloc[0:24]
            validRecordsLength = 23 -checkValidRecords(df)


            if validRecordsLength < 12: #if the total records shorter than 12 hours, this station should not create any update
                print 'Too many missing records,NO observation records will be uploaded for Station ', STATION, Stations[STATION]
                logging.info("Too many missing records,NO observation records will be uploaded for Station: %s%s",STATION,Stations[STATION])
                return

            else:
            #Loop through the sliced valid records (including the previous 24 hours)
                for hour in range(validRecordsLength,-1,-1): #change the sequence from later to latest
                    df = asos[hour:hour+24]

                    #Remove records with temperature and dewpoint with no data
                    if df.isnull().values.any() == True:
                        print 'there are nodata value'
                        df.dropna(axis=0, how='any', inplace=True)


                    #For each record, the historical records should not be excceed one day
                    if len(checkMissingRecords(df)) >0:
                        #print 'Processing '

                        #Do numerical calculation for relative humidity, precipitation, duration and windspeed reduction
                        df.loc[:,'RH']= df.apply(RH,axis = 1)
                        #Regarding the rain duration, onyly > 0.005 will be recorded(so 0.005 should be disregarded) Ask Mike or Brad how they did with RAWS observation
                        df.loc[:,'precip_in']=df['precip_in'].apply(CorrectPrcpAmount)
                         #To define the Precipitation Duration hours
                        df.loc[:,'precip_duration']=df['precip_in'].apply(precipDuration)
                        #Wind speed at 6m, only pick up the 13 hours wind speed!!
                        df.loc[:,'WindSpeed']=df['wind_speed_kt'].apply(windspeed)
                        #Gust speed
                        df.loc[:,'GustSpeed']=df['wind_gust_kt'].apply(windspeed)

                        #Transform the current row into a dictionary for better operation
                        currenthour= df[:1].set_index('station_id').T.to_dict()
                        currenthour = currenthour[STATION]

                        ##Pass the previous 24 hours and current hour record for WIMS fw9 format
                        Report13(df,X13,currenthour)
                        ##Write the records into a FW13 and FW9 format
                        F13.write( FormatFW13( X13 ) +'\n' )
                        ##Pass the previous 24 hours and current hour record for WIMS fw13 format
                        Report9(df,X9,currenthour)
                        ##Write the records into a FW13 and FW9 format
                        F9.write( FormatFW9( X9 ) +'\n' )

                    else:
                        print "There is an unvalid record for Station: ",(STATION)
                        logging.info("There is an unvalid record for Station: %s",STATION)
#-------------------------------------------------------------------------------
# Start from here
#-------------------------------------------------------------------------------
#Updated on 03/11/2016 for make the time flexible
#Updated on 03/07/2017 for the day saving time
#####################################################
#-------------------------------------------------------------------------------
#Define primary workspace based on location of script
WorkSpace = os.getcwd()
LOGfile = os.path.join(WorkSpace,"ASOS4WIMS.log")
#The downloaded file and processed file will be in the HIST directory
ASOSArchive = os.path.join(WorkSpace, "CSV")
FW13Archive  = os.path.join(WorkSpace, "FW13")
FW9Archive  = os.path.join(WorkSpace, "FW9")
LOGArchive  = os.path.join(WorkSpace, "LOG")

if not os.path.exists(ASOSArchive):
    os.makedirs(ASOSArchive)
if not os.path.exists(FW13Archive):
    os.makedirs(FW13Archive)
if not os.path.exists(FW9Archive):
    os.makedirs(FW9Archive)
if not os.path.exists(LOGArchive):
    os.makedirs(LOGArchive)

#define the fire weather file name
fileWF13 = os.path.join(WorkSpace, "tx-asos.fw13")
fileWF9 = os.path.join(WorkSpace, "tx-asos.fw9")
#for each day first removing the existing file
if os.path.isfile(fileWF13):
    os.remove(fileWF13)
#for each day first removing the existing file
if os.path.isfile(fileWF9):
    os.remove(fileWF9)
#Station list for the 21 stations that provided by Mike
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
#                "KCRS": 412001,
            "KTYR": 411701,
            "KTRL": 419703,
            "KDTO": 419603,
            "KMWL": 419404}

#Stations = {"KLBB": 419002}
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
        shrubGreennessF[int(row[6])]=int(row[7])
        herbaceousGreennessF[int(row[6])]=int(row[8])
        seasonCode[int(row[6])]=int(row[9])

#Set up a logger for logging the running information
logging.basicConfig(filename=LOGfile,
                    format='%(asctime)s   %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p',
                    filemode='w',
                    level=logging.INFO)
#Check the current time for running, to see if it is the ZULU time of 19:00 hour
# set up date information
today = datetime.datetime.today()

START_UTC = "T19:00:00Z"
END_UTC = "T18:59:59Z"
#today = date(datetime.now())
# set up date information
today = datetime.datetime.today()
logging.info("Start ASOS processing for %s", today.strftime("%Y%m%d"))
#logging.info("Start ASOS processing for %s", datetime.now().strftime("%Y%m%d%H"))
UTC_now = datetime.datetime.utcnow()
UTCHOUR_now = int(UTC_now.strftime("%H"))
#How about using the UTC hour (should always be 19:00) to avoid day saving issue
if UTCHOUR_now >= 19:
## Process today's data
    TODATSTR = today.strftime("%Y-%m-%d")
    two_day = datetime.timedelta(days=2)
    Twodaybeforetoday = today - two_day
    TWODAYSTR = Twodaybeforetoday.strftime("%Y-%m-%d")

else:
    three_day = datetime.timedelta(days=3)
    one_day = datetime.timedelta(days=1)
    ThreeDay = today - three_day
    YesterDay = today - one_day
    TODATSTR = YesterDay.strftime("%Y-%m-%d")
    TWODAYSTR = ThreeDay.strftime("%Y-%m-%d")

StartTime  =  TWODAYSTR + START_UTC
EndTime = TODATSTR + END_UTC
print "StartTime: ",StartTime,"EndTime: ",EndTime

##Stations = {"KLBB": 419002}
for STATION,ID in Stations.items():
    try:
        #Updated to 48 hour to calculate the 24-hour summary for each hour
        #URL = 'http://www.aviationweather.gov/adds/dataserver_current/httpparam?dataSource=metars&requestType=retrieve&format=csv&hoursBeforeNow=%d&stationString=%s'%(hoursBeforeNow,STATION,)
        #Using a specified time frame for the inquery instead
        URL='http://www.aviationweather.gov/adds/dataserver_current/httpparam?dataSource=metars&requestType=retrieve&format=csv&startTime=%s&endTime=%s&stationString=%s'%(StartTime,EndTime,STATION,)
        print URL
        print 'Downloading ASOS data for Station: ' + STATION
        filename = "%s-%s.csv"%(STATION,today.strftime("%Y%m%d%H%M"))
        csvfile = os.path.join(ASOSArchive, filename)
        print(csvfile)
        urllib.urlretrieve(URL,csvfile)
    except:
        # Update 11102016 for better dealing with download issues
        # Catch the error type from the urllib.urlretrieve(URL,csvfile)
        MSG = "The ASOS source data were not downloaded successfully for Station: %s %s"% (STATION,Stations[STATION])
        print(MSG)
        logging.info(MSG)
        sendEmail(MSG)
        #Should not exit here, use continue
        contine
    #Inteprete ASOS data for WIMS instake
    #IntepreteASOS(csvfile,STATION,ID)
    try:
        print 'Processing ASOS Station: ' + STATION + ' with Station ID: ' + str(ID)
        IntepreteASOS(csvfile,STATION,ID)
    except:
        MSG = "The ASOS data were not processed successfully for Station: %s %s"% (STATION,Stations[STATION])
        logging.info(MSG)
        print "Unexpected error:", sys.exc_info()[0]
        sendEmail(MSG)

#Create Archive folders for the source data and resulting data
ASOSArchive = os.path.join(WorkSpace, "CSV")
FW13Archive  = os.path.join(WorkSpace, "FW13")
FW9Archive  = os.path.join(WorkSpace, "FW9")
LOGArchive  = os.path.join(WorkSpace, "LOG")

#Archive the WF9 file for each day
archivefileWF9 = today.strftime("%Y%m%d") + ".fw9"
archivefileWF9 = os.path.join(FW9Archive, archivefileWF9)
shutil.copyfile(fileWF9,archivefileWF9)
#Archive the WF13 file for each day
archivefileWF13 = today.strftime("%Y%m%d") + ".fw13"
archivefileWF13 = os.path.join(FW13Archive, archivefileWF13)
shutil.copyfile(fileWF13,archivefileWF13)
#Keep Record of the log file
archivefileLOG = today.strftime("%Y%m%d") + ".log"
archivefileLOG = os.path.join(LOGArchive, archivefileLOG)
shutil.copyfile(LOGfile,archivefileLOG)
