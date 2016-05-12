HELP document on the daily uploading of ASOS observations to WIMS Test:

Purpose:
The script download the hourly ASOS report from National Aviation Weather Center and generate the Fire Weather data format (FW13 and FW9) for 21 stations in TX. 

Requirements:
1. The script runs at 14:00 PM (Daylight Saving Time) and 13:00 PM (Standard time)  central time zone driven by a scheduled task on Windows 7 (the time window can be :00 to :45 but not suggested during the station reporting time e.g. :50~:00)
2. Several python libraries including latest Pandas(version 0.17.1) need to be installed as a dependency, I used "pip install pandas" for easy installation. A trouble shooting page on stack overflow (link: http://stackoverflow.com/questions/4750806/how-to-install-pip-on-windows) can help.
   There are also pre-compiled installations for the related libraries in SciPy.org (See link https://www.scipy.org/install.html for an alternative way)
3. A csv file (named here ASOS_SeasonCode_GreenessCode.csv) need to be prepared where the Shrub greenness factor/Herbaceous greenness factor/Season code for each station are specified.

Folder Structure (Will created after you run the program for ARCHIVING):
CSV:   The downloaded ASOS records up to 48 hours beyond in the CSV format are resided 
FW9:  The generated Fire Weather 9 format for each run are resided 
FW13: The generated Fire Weather 13 format for each run are resided 
LOG:   The log files for each run are resided
HIST: The result/source file of processing of historical ASOS(for historical breaking points set)  

Files:
README_ASOS2WIMS.txt: The instruction and help information 
run_asos4wims.bat: The batch file contains the commands to run python scripts and upload the FW9/13 files to the famtest webdav using a credential
ASOS2WIMS_TX.py: The python script for downloading the ASOS weather records and analyzing/computing fire weather parameters for the resulting FW9/FW13 file for submitting to the Famweb on daily operations. Email alert can be created if there are issues for this process
tx-asos.fw9: The generated fire weather files according to the FW9 format
tx-asos.fw13: The generated fire weather files according to the FW13 format
ASOS2WIMS_TX_HISTORY: The python script for downloading historical ASOS records and analyzing/computing fire weather parameters for the resulting FW13 file where the historical records can be retrived for setting up and thesholds and breakpoints.