HELP document on convertint XML file from WIMS WXML service to Fire Weather Format:

Purpose:
The script download the daily observation from WIMS WXML service and generate the Fire Weather data format (FW13) for RAWS stations. 

Requirements:
1. Python 2.x (the current version was tested on Windows 7 command line tool).
3. A csv file (named here tx_raws.csv) which contains station name and stationid in two columns for more stations (currently are 79 raws station in Texas).

Folder Structure (Will created after you run the program for ARCHIVING):
FW13: The generated Fire Weather 13 format for all the stations listed in the csv file
XML: The source file in the XML format downloaded from WIMS WXML service

Files:
README_XML2FW13: The instruction and help information 
run_asos4wims.bat: The batch file contains the commands to run python scripts and upload the FW9/13 files to the famtest webdav using a credential
XML2FW13.py: The python script for downloading download the daily observation from WIMS WXML service and generate the Fire Weather data format
TX-RAWS.fw13: The generated fire weather files according to the FW13 format

How to run:

For running on Windows Opering system, you may need to add the python2.x program directory into the Environment Variable "Path" then:

Open a windows command line window (or terminal in other operating system) and cd into the XMLFW13 folder, type:
python XML2FW13.py 9-May-2016 10-May-2016 
                      |            |
		      |            |
		      |            ---> end date formatted in day-month abbrievation(Jan, Feb,Mar,....)-year
                      |---> start date formatted in day-month abbrievation-year

Additional Tips:
If there are several versions of python installed in your computer, you may need to specify your Python2.x directory in the command line, type:
C:\Python27\ArcGIS10.4\python.exe XML2FW13.py 9-May-2016 10-May-2016 