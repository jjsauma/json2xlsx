import pandas as pd
from glob import glob
import os
import sys
from dateutil import parser

# will silence warnings to stop seeing
# messages of confirmed bugs in python. Will check later
# for appropiate solution (lines 33 and 34)
import warnings 
warnings.filterwarnings("ignore")

path = 'tvav_test-files'
epg = 'epg.csv'
chan = 'matches-*.json'

def tv(start, end, channel):
    
    # read csv file into pandas dataframe
    df = pd.read_csv(os.path.join(path, epg))
    
    # compute start_time and end_time for each row
    df['start'] = df[['start_date']].applymap(str).applymap (lambda s: "{}/{}/{}".format(s[0:4], s[4:6], s[6:]))
    df['start'] = pd.to_datetime(df['start'] + ' ' + df['start_time'])
    df['end'] = df['start'] + pd.to_timedelta(df['duration_in_seconds'], unit='s')
    
    # create and format start_time and end_time fields for report
      
    # data cleaning 
    # (dirty solution, until more sofisticated and 
    # smart approach is implemented)
    df['channel_name'] = df['channel_name'].str.lower()
    #df['channel_name'] = df['channel_name'].str.replace("*rte*", "rte", regex=True)
    #df['channel_name'] = df['channel_name'].str.replace("*hd1*", "hd1", regex=True)
    df['channel_name'][df.channel_name.str.contains('rte')] = 'rte'    
    df['channel_name'][df.channel_name.str.contains('hd1')] = 'hd1'
    
    # select records for the given arguments
    df = df[(df['start'] >= start) & (df['end'] <= end) & (df['channel_name'] == channel)] 

    return df

def songs(start, end, channel):

    # create pandas dataframe to save data
    df = pd.DataFrame(columns = ['album', 'artist', 'channel', 'length', 'start_time_utc', 'title'])
    
    # read files into pandas dataframe
    for file in glob(path + '/' + 'matches-*.json'):
        raw = pd.read_json(file, lines=True)
        raw['channel'] = file[file.find("matches-")+8:-5]
        df = df.append(raw)
    
    # data cleaning
    df['start_time_utc'] =  pd.to_datetime(df['start_time_utc'])
    
    df['channel'] = df['channel'].str.replace("_", " ")
    df['channel'] = df['channel'].str.lower()
    
    # filter according to parameters    
    df = df[(df['start_time_utc'] >= start) & (df['start_time_utc'] <= end) & (df['channel'] == channel)]
    
    return df

def main(argv):
    # example of use: python music_report1.py '2018-01-01 10:00' '2018-12-31 13:45' 'rte'
    
    print("Processing...")
    start = parser.parse(argv[0])
    end = parser.parse(argv[1])
    channel = argv[2]
    
    dfResult = pd.DataFrame(columns = ['Program Start', 'Program Title', 'Song Title', 'Song Artist', 'Song Start', 'Song Duration'])
    
    # retrieve TV data
    dfTV = tv(start, end, channel)
    
    # retrieve song information
    dfSongs = songs(start, end, channel)
    
    # find songs played per program
    dfSongs['end'] = dfSongs['start_time_utc'] + pd.to_timedelta(dfSongs['length'], unit='s')
    dfTV = dfTV.groupby(['program_original_title', 'start'])['program_original_title', 'start', 'end'].first()
           
    for i in range(len(dfSongs)):
        for j in range(len(dfTV)):
            s1 = dfSongs.iat[i, 4] #dfSongs.iloc[i]['start_time_utc']
            e1 = dfSongs.iat[1, 6] #dfSongs.iloc[i]['end'] 
            s2 = dfTV.iat[j, 1] #dfTV.iloc[j]['start']
            e2 = dfTV.iat[j, 2] #dfTV.iloc[j]['end']
            if s1 >= s2 and s1 < e2 and e1 < e2:
                dfResult = dfResult.append({
                        'Program Start': s2, 
                        'Program Title': dfTV.iat[j, 0], #dfTV.iloc[j]['program_original_title'], 
                        'Song Title': dfSongs.iat[i, 5], # dfSongs.iloc[i]['title'], 
                        'Song Artist': dfSongs.iat[i, 1], #dfSongs.iloc[i]['artist'], 
                        'Song Start': s1, 
                        'Song Duration': dfSongs.iat[i, 2], #dfSongs.iloc[i]['length'] 
                            }, ignore_index=True)

    
    dfResult.to_excel('music_report.xlsx')
    print("Report saved in file music_report.xlsx")

if __name__ == "__main__":
    main(sys.argv[1:])
