import requests
import json
import os

# Configuration

fail_filename = "log_failed.txt"
download_directory = "data"

# Initialization

urlList = []
downloaded = 0
attempts = 0

# Helper functions

def downloadFailedData(directory, urlList):
    global downloaded, failed, emptyJsons

    tryagain = []
    
    for url in urlList:                                                 # Get reporting type of the station from the url
        reportshelper = url.split('history/')[1].split('?')[0]          # To create new folder under the download directory

        foldernamehelper = []

        for elem in url.split('date=')[1:]:                             # Get date from the url
            foldernamehelper.append(elem.split('&')[0])                 # And use it as folder name under the folder created above
        foldername = foldernamehelper[0] + '_' + foldernamehelper[1]
        
        path = directory + '/' + reportshelper + '/' + foldername       # This folder should be created under the reporting type folder
        
        try:
            if not os.path.exists(path):                                    # Create the folder named according to the date interval
                os.makedirs(path)                                           # If it does not already exist
        except:
            pass

        filename = url.split("station=")[1].split("&")[0]               # Get file name from the link 
        filename = path + '/' + filename + '.json'                      # Use this name to name json file.

        #print(url)

        try:
            response = requests.get(url)                                # Get the info from the server
            outfile = open(filename, "w")                               # Create the file named above
            # If empty json is not wanted, move above line
            try:
                json_response = response.json()                             # Process it as json
                json.dump(json_response, outfile)                           # Write processed info as json
                downloaded += 1
                outfile.close()
                print(filename + " downloaded without any problem")
            except:
                downloaded += 1
                emptyJsons += 1
                outfile.close()
                print(filename + " downloaded but it is empty (website can't provide any information)")
                continue
        except:
            #print("Cannot download " + url + ". Logged to log_failed.txt")
            tryagain.append(url)

    return tryagain

    if (tryagain != []):
        moreattempts = 'y'
        print(attempts % 5)
        if ((attempts % 5 == 0) & attempts!=0):
            print("Retried " + str(attempts) + " times, but couldn't download all the files.")
            print("Do you want to try again? (y/n)")
            moreattempts = input()

        if (moreattempts == 'y'):
            print("\n\n\nTrying again...")
            attempts += 1
            downloadFailedData(directory, tryagain)
        else:
            fail_file = open(fail_filename, "w")
            for url in tryagain:
                fail_file.write(url + '\n')
        

# Main

fail_file = open(fail_filename, "r")

for line in fail_file:
    urlList.append(line.split('\n')[0])

tryagain = downloadFailedData(download_directory, urlList)

while (tryagain != []):
    moreattempts = 'y'
    if ((attempts % 5 == 0) & (attempts != 0)):
        print("Retried " + str(attempts) + " times, but couldn't download all the files.")
        print("Do you want to try again? (y/n)")
        moreattempts = input()
    if (moreattempts == 'y'):
        print("\n\n\nTrying again...")
        attempts += 1
        downloadFailedData(download_directory, tryagain)
    else:
        fail_file = open(fail_filename, "w")
        for url in tryagain:
            fail_file.write(url + '\n')
        break

fail_file = open(fail_filename, "w")
fail_file.close()

print("No failed files left")
