#-------------------------------------------------------------------------------
# Name:      XML2FW13.py
# Purpose:   Transform Obs from WXML feed into FW13 format for a station
# Author:    Peter Yang
# Created:   05/10/2015
#-------------------------------------------------------------------------------
import urllib
import xml.etree.ElementTree as ET
import datetime
import os
import string
import sys

#------------------------------------------------------------------------------------------------
# Functions for writing the station dic to a csv(or text file with ',' delemitated)
# Input: Dictionary (X) that contains station name and station id
# Output: A csv file with two columns for station name and staiton id
#-----------------------------------------------------------------------------------------
def saveDict(fn,dict_rap):
    f=open(fn, "wb")
    w = csv.writer(f)
    for key, val in dict_rap.items():
        w.writerow([key, val])
    f.close()
#------------------------------------------------------------------------------------------------
# Functions for reading the station list from a csv(or text file with ',' delemitated)
# Input: A csv file with two columns for station name and staiton id
# Output: Dictionary (X) that contains station name and station id
#--------------------------------------------------------------------------------------------
def readDict(fn):
    f=open(fn,'rb')
    dict_rap={}
    for key, val in csv.reader(f):
        dict_rap[key]=eval(val)
    f.close()
    return(dict_rap)

#--------------------------------------------------------------------
# Function for Formatting the extracted information to the wf13 format
# Input: Dictionary (X) that contains all the information
# Output: A string with fw13 format
#-------------------------------------------------------------------
def FormatFW13( X ):
    Fields = (('W13',(0,'3A')),('sta_id',(3,'6A')),('obs_dt',(9,'8A')),('obs_tm',(17,'4A')),
              ('obs_type',(21,'1A')),('sow',(22,'1N')),('dry_temp',(23,'3N')),('rh',(26,'3N')),
              ('wind_dir',(29,'3N')),('wind_sp',(32,'3N')),('10hr Fuel',(35,'2N')),('temp_max',(37,'3N')),
              ('temp_min',(40,'3N')),('rh_max',(43,'3N')),('rh_min',(46,'3N')),('pp_dur',(49,'2N')),
              ('pp_amt',(51,'5N')),('wet',(56,'1A')),('grn_gr',(57,'2N')),('grn_sh',(59,'2N')),
              ('MoistType',(61,'1N')),('MeasType',(62,'1N')),('season_cd',(63,'1N')),('Solar_Radiation',(64,'4N')),
              ('Wind_Dir_Peak',(68,'3N')),('Wind_Speed_Peak',(71,'3N')),('snow_flg',(74,'1A'))
              )

    Out = []
    for f,p in Fields:
        val = X[f]
        #str(X[f]).zfill()
        length = int(p[1][:-1]) #not working
        format = p[1][-1]

        if f=='pp_amt':
            if val == 0:
                val=-999
            else:
                val*=1000
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

#-----------------------------------------------------------------------------------------
# Parse the XML feeds into Fire Weather 13 format
#-----------------------------------------------------------------------------------------
def ParseXML(station,XMLFileName):
    # Header from the XML file
    header0 = ['sta_id', 'obs_dt', 'obs_tm', 'obs_type','sow','dry_temp','rh','wind_dir',
                'wind_sp','temp_max','temp_min','rh_max','rh_min','pp_dur','pp_amt','wet','season_cd','grn_gr','grn_sh','snow_flg','Solar_Radiation','Wind_Dir_Peak','Wind_Speed_Peak']

    #Dictionary for holding all the parameter in the FW13 format
    X13 = {'W13':'W13', 'sta_id':'000000', 'obs_dt':'YYYYMMDD', 'obs_tm':13,
      'obs_type':'O', 'sow':0, 'dry_temp':0, 'rh':0,
      'wind_dir':0, 'wind_sp':0, '10hr Fuel':0, 'temp_max':-999,
      'temp_min':-999, 'rh_max':-999, 'rh_min':-999, 'pp_dur':0,
      'pp_amt':0, 'wet':'N', 'grn_gr':20, 'grn_sh':15,
      'MoistType':2, 'MeasType':1, 'season_cd':3, 'Solar_Radiation':0,
      'Wind_Dir_Peak':0,'Wind_Speed_Peak':0,'snow_flg':'N'
    }
    ##parsing the XML file using ET
    tree = ET.parse(XMLFileName)
    root = tree.getroot()
    print "Parsing XML for FW13 formating for station : %s  " %(station)
    filewf13 = os.path.join(fileWF13, "TX-RAWS.fw13")
    with open(filewf13,'a') as F13:
        if not root.getchildren():
            MSG = "There is a problem of RAWS observation for station in WIMS: %s "% (station)
            print MSG
        else:
            for row in root.findall('row'):
                for field in header0:
                    ##for some reason, some stations doesn't have the grn_gr, season_cd and grn_sh fields
                    if row.find(field) is None:
                        MSG = "%s is not derived for station %s "%(field,station)
                        #print MSG
                    else:
                        if field in ['wind_dir','rh_max','pp_dur','wind_sp','sow','temp_min','GustDir','temp_max','dry_temp','season_cd','rh','grn_sh','rh_min','grn_gr']:
                            X13[field] = int(row.find(field).text)
                        elif field in ['obs_dt']:
                            date = row.find(field).text
                            YYMMDD = date[6:] + date[0:2] + date[3:5]
                            X13[field] = YYMMDD
                        elif field in ['obs_tm']:
                            time = row.find(field).text
                            TIME = time + '00'
                            X13[field] = TIME
                        elif field in ['pp_amt']:
                            X13[field] = float(row.find(field).text)
                        else:
                            X13[field] = row.find(field).text
                ##print X13
                F13.write( FormatFW13( X13 ) +'\n' )

#-----------------------------------------------------------------------------------------
# Download the XML files for each station from WIMS Observation WXML service
#-----------------------------------------------------------------------------------------
def DownloadASOS(station,stationid,start,end):

    # define xml file locations
    downloadtime = datetime.datetime.now().strftime("%Y%m%dH%HM%M")
    filename = str(stationid) + "_"+ start + "-" + end + "-" + downloadtime + "_obs.xml"
    xmlobs = os.path.join(fileXML, filename)

    # Get reponse from WIMS server
    serverResponse = urllib.urlopen('https://famtest.nwcg.gov/wims/xsql/nfdrs.xsql')
    # Check reponse code
    # If WIMS server is available, download xml files. Otherwise, exit process.
    if serverResponse.getcode() == 200:
        # Observations
        url = "https://fam.nwcg.gov/wims/xsql/obs.xsql?stn=" + str(stationid) + "&sig=&type=O&start=" + start + "&end=" + end + "&time=&sort=asc&ndays=&user="
        urllib.urlretrieve(url,xmlobs)

        print "Downloading WXML for station : %s withid: %s" %(station, str(stationid))
        #Parse the downloaded XML file FW13 fomatting
        ParseXML(station,xmlobs)
    else:
        print 'WIMS System is unavailable'
        raise SystemExit()

##Get the working directory and create folders for XML and FW13 files
WorkSpace = os.getcwd()
fileXML = os.path.join(WorkSpace, "XML")
fileWF13  = os.path.join(WorkSpace, "FW13")
if not os.path.exists(fileXML):
    os.makedirs(fileXML)
if not os.path.exists(fileWF13):
    os.makedirs(fileWF13)

# A csv (or text) file has the station name and station ID
stationfile = os.path.join(WorkSpace,'tx_raws.csv')

try:
    RAWS_TX = readDict(stationfile)

except:
    #A list of weather stations with station id for RAWS in TX
    RAWS_TX = {  'ANAHUAC NWR': 416099,
                 'ARANSAS': 418502,
                 'ATHENS': 412101,
                 'ATTWATER': 416601,
                 'BALCONES CANYONLANDS': 417902,
                 'BALCONES FLYING X': 417903,
                 'BARNHART': 417701,
                 'BASTROP': 415501,
                 'BIRD': 417901,
                 'BOOTLEG': 418801,
                 'BRAZORIA': 418301,
                 'CADDO': 410202,
                 'CADDO LAKE NWR': 411901,
                 'CAPROCK SP': 418901,
                 'CEDAR': 418701,
                 'CEDAR HILL S.P.': 419701,
                 'CHISOS': 417403,
                 'CLARKSVILLE': 410401,
                 'COLDSPRINGS': 414201,
                 'COLEMAN': 419502,
                 'COLORADO BEND_SP': 419501,
                 'COMANCHE': 419403,
                 'CONROE': 415109,
                 'DAYTON': 415201,
                 'EAST AUSTIN': 417904,
                 'ELEPHANT MTN. WMA': 417404,
                 'FORT DAVIS': 417201,
                 'GAIL': 419101,
                 'GEORGE WEST': 418201,
                 'GILMER': 411401,
                 'GRANBURY': 419702,
                 'GREENVILLE': 419602,
                 'GUADALUPE RIVER SP': 418101,
                 'HAMBY': 419401,
                 'HEBBRONVILLE': 418401,
                 'HENDERSON': 412202,
                 'HUNTSVILLE': 414102,
                 'JAYTON': 419001,
                 'KICKAPOO CAVERNS SP': 418001,
                 'KIRBYVILLE': 414501,
                 'LA PUERTA': 418604,
                 'LAGRANGE': 415602,
                 'LAGUNA ATASCOSA': 418603,
                 'LBJ': 419601,
                 'LINDEN': 411102,
                 'LINN-SAN MANUEL': 418605,
                 'LOST MAPLES SNA': 417802,
                 'LUFKIN': 413509,
                 'LUMBERJACK': 412801,
                 'MASON': 417801,
                 'MATADOR WMA': 418902,
                 'MCFADDIN NWR': 419901,
                 'MCGREGOR': 419802,
                 'MERRILL': 418002,
                 'MIDLAND': 419202,
                 'MILLER CREEK': 419301,
                 'NEASLONEY WMA': 416401,
                 'PAINT CREEK': 419203,
                 'PALESTINE': 412601,
                 'PANTHER JUNCTION': 417401,
                 'PEARSALL': 418102,
                 'PINERY': 417101,
                 'POSSUM KINGDOM_SP': 419402,
                 'PX WELL': 417105,
                 'RATCLIFF': 413302,
                 'ROUND PRAIRIE': 413101,
                 'SABINE NORTH': 412901,
                 'SABINE SOUTH': 413701,
                 'SAN BERNARD': 418302,
                 'SANTA ANA NWR': 418602,
                 'SOUTH AUSTIN': 417905,
                 'SOUTHERN ROUGH': 416101,
                 'TEMPLE': 419801,
                 'TEXARKANA': 410501,
                 'THE BOWL': 417103,
                 'VICTORIA': 418202,
                 'WHEELER': 418802,
                 'WOODVILLE': 414402,
                 'ZAVALLA': 413503
        }

##RAWS_TX = { 'PX WELL': 417105}
##RAWS_TX = {'ANAHUAC NWR': 416099}
##RAWS_TX = {'WHEELER':418802}
##Create a simple interface in command line to provide start and end days to download (otherwize 7 days from now)
## The start and end day format should be day-month abbreciation-year  for e.g. today is 05/11/2016 the input should be 11-May-2016
if len(sys.argv) > 2:
    start = sys.argv[1]
    end  = sys.argv[2]
    print 'Start Date:', start, ' End Date: ',end
else:
    #Get today's date
    print 'Start Date and End not provided, Starting processing 7 days before today'
    today = datetime.datetime.today()
    #Seven day from now (also can be changed to 30,60,90 or 180 days)
    seven_day = datetime.timedelta(days=7)
    sevenday = today - seven_day
    end = today.strftime("%d-%b-%y")
    start = sevenday.strftime("%d-%b-%y")
    print 'Start Date:', start, ' End Date: ',end
    ##sixty_day = datetime.timedelta(days=60)
    ##ninety_day = datetime.timedelta(days=90)
    ##hundredeighty_day = datetime.timedelta(days=180)
    ##one_day = datetime.timedelta(days=1)
    ##yesterday = today - one_day
    ##sixtyday = today - sixty_day
    ##ninetyday = today - ninety_day
    ##hundredeightyday = today - hundredeighty_day
    ##start = yesterday.strftime("%d-%b-%y")
    ##start = sixtyday.strftime("%d-%b-%y")
    ##start = ninetyday.strftime("%d-%b-%y")
    ##start = hundredeightyday.strftime("%d-%b-%y")

#----------------------------------------------------------------------------------
#Processing 79 RAWS station in TX with provided station name and station id
#---------------------------------------------------------------------------------
for station,stationid in RAWS_TX.items():
    print station,stationid
    DownloadASOS(station,stationid,start,end)
