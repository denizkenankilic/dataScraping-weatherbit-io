import csv
import json
import os
import requests
import threading
import math
import time
from datetime import date, timedelta

# ------ CONFIGURATION ------ #

# API

API_KEY = 'YOUR_WEATHERBIT_API' # Edit here by using your API key
urlBase = "https://api.weatherbit.io/v2.0/history/"

# Input - Output

GET_INPUT = True
failed = 0

# Below are not important as long as GET_INPUT == True

running_mode = "n"
stationFile = "stations.csv"
downloadDirectory = "data"
threadCount = 50
failedFilename = "log_failed.txt"
contFilename = "checkpoint.txt"
percentageFilename = "percentage.txt"

if not os.path.exists(failedFilename):
    os.makedirs(failedFilename)
if not os.path.exists(contFilename):
    os.makedirs(contFilename)
if not os.path.exists(percentageFilename):
    os.makedirs(percentageFilename)

downloadSubhourly = False
downloadHourly = True
downloadDaily = True

startDate = date(2009, 6, 1)
endDate = date(2010, 5, 31)

# ------ OBJECT TYPES ------ #

class station:
    def __init__(self, id, lat, lon, source, reports, country):
        self.id = id                   # station id
        self.lat = lat                 # station latitude
        self.lon = lon                 # station longitude
        self.source = source           # station source
        self.reports = reports         # station report type
        self.country = country         # station country code

# ------ HELPER FUNCTIONS ------ #

def readStations(stationFile):
    '''
    This function reads station information from a csv file.
    CSV file should be in the same form with weatherbit.io's
    stations file.

    Input  : A csv file directory (of type string)
    Output : List of stations (of type List[station])
    '''
    stations = []
    rows = []

    csvfile = open(stationFile)                                             # Open CSV file
    stationreader = csv.reader(csvfile, delimiter=',', quotechar="\"")      # Read data from CSV file by using Python's csv library
    next(stationreader)                                                     # Skip first line (header)

    for row in stationreader:                                               # Move data from csv.reader to a list
        rows.append(row)                                                    # Since csv.reader is not indexable

    for row in rows:                                                        # Initialize stations from the list
        newStation = station(row[0], float(row[1]), float(row[2]), row[3], row[4], row[5])
        stations.append(newStation)                                         # Add them to return list

    print(str(len(stations)) + " stations are read from the file.")

    return stations

def filterStations(stations):
    '''
    This function filters stations to eliminate useless data.
    Returns the subhourly, hourly, and daily working stations.

    Input  : A list of stations (of type List[station])
    Output : 3 lists of stations (of type List[station])
    '''
    subhourlyStats = []         # List of subhourly reporting stations
    hourlyStats = []            # List of hourly reporting stations
    dailyStats = []             # List of daily reporting stations

    for station in stations:
        if ((station.lat == 0.0) & (station.lon == 0.0)):       # If station information is not applicable, skip it.
            continue
        else:
            if (station.reports == "subhourly"):                # If station reports subhourly, add it to subhourly reporting stations list
                subhourlyStats.append(station)
            elif (station.reports == "hourly"):                 # If station reports hourly, add it to hourly reporting stations list
                hourlyStats.append(station)
            elif (station.reports == "daily"):                  # If station reports daily, add it to daily reporting stations list
                dailyStats.append(station)

    statCount = len(stations)
    subhourlyCnt = len(subhourlyStats)
    hourlyCnt = len(hourlyStats)
    dailyCnt = len(dailyStats)

    erroneous = statCount - subhourlyCnt - hourlyCnt - dailyCnt

    print( str(statCount) + " stations are filtered into " + str(subhourlyCnt) + " subhourly, " + str(hourlyCnt) + " hourly, and " + str(dailyCnt) + " daily reporting stations." )
    print( str(erroneous) + " problematic stations are eliminated." )

    return subhourlyStats, hourlyStats, dailyStats

def downloadWeatherData(directory, urlList):
    '''
    This function downloads the json files of the desired
    stations, according to the list of urls.

    It classifies the daily, hourly, and subhourly data in
    different folders. Under these folders, there will be
    new folders named according to the date of information
    they keep. (30-day-long periods)

    Downloaded json files will be kept in these folders, and
    will be named as their station id.

    Input  : Download directory (of type string)
    Output : List of urls (of type List[str])

    '''
    global downloaded, failed, emptyJsons

    failedFile = open(failedFilename, "a")                              # Clear failed downloads
    failedFile.close()

    for url in urlList:                                                 # Get reporting type of the station from the url
        reportshelper = url.split('history/')[1].split('?')[0]          # To create new folder under the download directory

        foldernamehelper = []

        for elem in url.split('date=')[1:]:                             # Get date from the url
            foldernamehelper.append(elem.split('&')[0])                 # And use it as folder name under the folder created above
        foldername = foldernamehelper[0] + '_' + foldernamehelper[1]

        path = directory + '/' + reportshelper + '/' + foldername       # This folder should be created under the reporting type folder

        try:
            if not os.path.exists(path):                                # Create the folder named according to the date interval
                os.makedirs(path)                                       # If it does not already exist
        except:
            pass

        filename = url.split("station=")[1].split("&")[0]               # Get file name from the link
        filename = path + '/' + filename + '.json'                      # Use this name to name json file.

        try:
            response = requests.get(url)                                # Get the info from the server
            outfile = open(filename, "w")                               # Create the file named above
            # If empty json is not wanted, move above line
            try:
                json_response = response.json()                         # Process it as json
                json.dump(json_response, outfile)                       # Write processed info as json
                downloaded += 1
                percentage = 100*downloaded/fileCount
                outfile.close()
            except:
                downloaded += 1
                emptyJsons += 1
                outfile.close()
                continue
            print(str(downloaded) + " of " + str(fileCount) + " files are downloaded (" + "{:.2f}".format(percentage) + "%)")

        except:
            print("Cannot download " + url + ". Logged to " + failedFilename)
            failedFile = open(failedFilename, "a")
            failedFile.write(url + "\n")
            failed += 1


def urlCreator(station, startDate, endDate):
    '''
    Creates download urls for given station, start date,
    and end date.

    Input  : station, starting date, end date (of types station, date, date)
    Output :
    '''
    dates = []
    urls = []

    dates.append(str(startDate))                                        # Add start date to the list
    totalDays = (endDate - startDate).days                              # Calculate how many days are there in the time interval as integer
    i = 31
    while (totalDays > 31):                                             # Add all the dates to the list
        dates.append(str(startDate + timedelta(i)))                     # Until less than 31 days are left
        i += 31                                                         # Since we can request at most 30 days in one query
        totalDays -= 31

    dates.append(str(endDate))                                          # Add end date to the list

    for oneDate, dateBefore in zip(dates[1:], dates[:-1]):              # Create the URL in the form of weatherbit.io's queries
        url = urlBase + station.reports + "?station=" + station.id + "&start_date=" + dateBefore + "&end_date=" + oneDate + "&key=" + API_KEY
        urls.append(url)

    return urls

def handle_input():
    '''
    This function handles the input operations
    '''
    global downloadSubhourly, downloadHourly, downloadDaily, startDate, endDate, downloadDirectory, stationFile, threadCount, running_mode

    print("Start new download / Continue previous download (Enter: n for new, c for previous)")
    running_mode = input()

    if (running_mode == "n"):
        print ("Enter a csv file to read stations, or 1 to use 'stations.csv'")
        read_file_raw = input()

        print ("Which data do you want to download? (subhourly, hourly, daily) (Enter: 111 for all, 000 for none)")
        print ("subhourly is NOT recommended")
        downloadOptions = input()

        print ("Enter start date of the data (e.g. 2016-1-10)")
        start_date_raw = input()

        print ("Enter end date of the date (e.g. 2018-6-15)")
        end_date_raw = input()

        downloadSubhourly = False
        downloadHourly = False
        downloadDaily = False

        downloadOptions = str(downloadOptions)
        if (downloadOptions[0]=='1'):
            downloadSubhourly = True
        if (downloadOptions[1]=='1'):
            downloadHourly = True
        if (downloadOptions[2]=='1'):
            downloadDaily = True

        start_date = start_date_raw.split('-')
        startDate = date(int(start_date[0]), int(start_date[1]), int(start_date[2]))

        end_date = end_date_raw.split('-')
        endDate = date(int(end_date[0]), int(end_date[1]), int(end_date[2]))

        if (read_file_raw == '1'):
            stationFile = 'stations.csv'
        else:
            stationFile = read_file_raw

    print ("Enter a directory (without '/' at the end) to download, or 1 to download data to this_directory/data")
    directory_raw = input()

    print("How many threads do you want to use while downloading? (recommended: 50)")
    thread_count_raw = input()

    if (directory_raw == '1'):
        downloadDirectory = 'data'
    else:
        downloadDirectory = directory_raw

    threadCount = int(thread_count_raw)-1   # -1 since last thread will download the remaining files if (fileCount % threadCount != 0)

# ------ MAIN ------ #

downloadList = []
urlList = []
downloaded = 0
emptyJsons = 0
fileCount = 0

if (GET_INPUT):                             # Instead of getting input, configuration can be made at top of this file
    handle_input()


if (running_mode == "n"):
    stations = readStations(stationFile)    # Read station information from the CSV file
    subhourlyStations, hourlyStations, dailyStations = filterStations(stations)

    if (downloadSubhourly):                 # If downloadSubhourly == True, download subhourly info !!!!! NOT WORKING !!!!!
        for station in subhourlyStations:
            downloadList.append(station)
    if (downloadHourly):                    # If downloadHourly == True, download info from hourly reporting stations
        for station in hourlyStations:
            downloadList.append(station)
    if (downloadDaily):                     # If downloadDaily == True, download info from daily reporting stations
        for station in dailyStations:
            downloadList.append(station)

    for item in downloadList:                           # For all the files decided to be downloaded
        urls = urlCreator(item, startDate, endDate)     # Create the corresponding urls
        for url in urls:
            urlList.append(url)
    '''
    dlcount = 1363318
    liste = urlList[dlcount:]
    dosya = open('continue2.txt', 'w')
    for elem in liste:
        dosya.write(elem + '\n')
    dosya.close()
    '''
    fileCount = len(urlList)

else:
    checkpoint = open(contFilename, "r")
    for line in checkpoint:
        url = line.split("\n")[0]
        urlList.append(url)
    checkpoint.close()
    percentCheckpoint = open("percentage.txt", "r")
    downloaded = int(percentCheckpoint.readline().split('\n')[0])
    fileCount = int(percentCheckpoint.readline())
    percentCheckpoint.close()
    for i in range(100):
        print(fileCount)


try:
    response = requests.get("http://api.weatherbit.io/v2.0/subscription/usage?key=" + API_KEY)
    json_response = response.json()
    remaining_calls = json_response["historical_calls_remaining"]
    remaining_time = json_response["historical_calls_reset_ts"]
    if (remaining_calls == 0):
        print("You have no remaining calls.")
        print("Calls will reset in " + str(remaining_time))
        print("Program will halt.")
        time.sleep(10)
        exit(0)
    else:
        print("You have " + str(remaining_calls) + " calls.")
except:
    print("No answer from the server for remaining calls.")
    print("Program will halt.")
    time.sleep(10)
    exit(0)

download_later=[]

if (fileCount <= remaining_calls):
    payload = math.floor(fileCount / threadCount)   # Count of files per thread (take floor value, so that remaining files will be handled by another thread)
    now_downloading = fileCount
else:
    payload = math.floor(remaining_calls / threadCount)
    now_downloading = remaining_calls
    download_later = urlList[remaining_calls:]
    urlList = urlList[:remaining_calls]

checkpoint = open(contFilename, "w")
for url in download_later:
    checkpoint.write(url + '\n')
checkpoint.close()

percentCheckpoint = open("percentage.txt", "w")
percentCheckpoint.write(str(downloaded + len(urlList)) + '\n')
percentCheckpoint.write(str(fileCount))
percentCheckpoint.close()

print(str(remaining_calls) + " files will be downloaded")
print(str(fileCount - remaining_calls) + " files will be downloaded later.")
print("Please start program in continue mode to download remaining files later.")

threads = []

start = time.perf_counter()                         # Start time counter

if (threadCount > now_downloading):
    threadCount = now_downloading

for thread_no in range(threadCount):                # Urls are divided into threadCount parts
    threadx = threading.Thread(target=downloadWeatherData, args=(downloadDirectory, urlList[thread_no*payload:(thread_no+1)*payload]))
    threads.append(threadx)
    threadx.start()

new_thread = threading.Thread(target=downloadWeatherData, args=(downloadDirectory, urlList[(threadCount * payload):]))
threads.append(new_thread)                          # Download from remaining urls (if fileCount % threadCount != 0)
new_thread.start()

for t in threads:                                   # Close all threads
    t.join()

elapsed_time = time.perf_counter() - start          # Calculate elapsed time

print("COMPLETE! " + str(downloaded) + " files are downloaded in " + str(elapsed_time) + " seconds.")
print(str(emptyJsons) + " of them are empty.")

if (failed == 0):
    print("No problematic files were observed")

else:
    print(str(failed) + " files could not be downloaded. See " + failedFilename)
