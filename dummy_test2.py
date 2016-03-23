#This script reads a list of compressed raw json files (tweets) and filter them out by selecting
# those tweets whose content is referring to any place name from our list
#*********************************** This section loads needed modules **************
import csv
from pandas import DataFrame, Series
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import json
import gzip
import sys   
#from glob import glob
import glob 
import os
import pandas as pd
import re
import ujson
from pandas.io.json import json_normalize
tweets = pd.DataFrame() #initialize data frames used for processing tweets content
tweets1 = pd.DataFrame()
tweets_data_in_json = []
counters = 0

path_all_placeNames ="/storage/ingomez/place_names/placeNames_concatenated_all_v1.1.csv" #load file with place names
#path ='/storage2/foreseer/twitter/gardenhose/raw/2014/7/*.gz'  #path where compressed json files are located
path ='/storage/ingomez/place_names/placeNames_2014/dirtest/*.gz' #path where dummy files are located
files_all1 = glob.glob(path)                        #load all json files name within a directory
files_all = files_all1[0:1]
tweets_data = []
csvfile = open(path_all_placeNames, 'rU')           #open csv with place names
place_names_reader = csv.reader(csvfile)            #read csv content and stores table of placenames metadata
for fname in files_all:                             #iterate in every single json file
            print 'working on:'+fname
            t0 = time.time()
            tweets_data = []
	    counter=0
            filenames =  str(fname)
            file_parse = filenames.split("/")
            file_name = file_parse[6]+"_output_placeNames.json" #create file name
            with gzip.open(fname, 'r') as f:                    #open compressed json file for reading
                for line in f:                                  #iterate and get every line in file
                    try:
                        tweet = ujson.loads(line)               #get one line of file
                        if tweet['lang']=='en':                 #get line of file if language is english
                            data = pd.DataFrame(json_normalize(tweet))#get nested objects
                            for row in place_names_reader:            #this line gets a place name from table
                                string_to_filter = '\\b'+row[0]+'\\b' #format place name string with regex using word boundaries
                                tweets_data_filtered=data[data['text'].str.contains(string_to_filter,case=False)==True] #search for place name string in the text field
                                tweets_data_filtered1=data[data['user.location'].str.contains(string_to_filter,case=False)==True] #search for place name string in the user's profile location field
                                size_text = len(tweets_data_filtered)               #get number of fields found in text field
                                size_profile =  len(tweets_data_filtered1)          #get number of fields found in profile field
                                if size_profile > 0:                                #if at least one record is returned from the text column search, get metadata of the placename i nto the data
                                    tweets_data_filtered1['place_name']=row[0]
                                    tweets_data_filtered1['place_address']=row[1]
                                    tweets_data_filtered1['place_city']=row[2]
                                    tweets_data_filtered1['place_state']=row[3]
                                    tweets_data_filtered1['place_zip']=row[4]
                                    tweets_data_filtered1['place_county']=row[5]
                                    tweets_data_filtered1['place_latitude']=row[6]
                                    tweets_data_filtered1['place_longitude']=row[7]
                                    tweets_data_filtered1['place_feature']=row[8]
                                    tweets1=tweets1.append(tweets_data_filtered1)   #append/store results to existing results
                                    counter=counter+1                               #counter only for reference of result returned
                                if size_text > 0:                                   #if at least one record is returned from the profile location column search, get metadata of the placename into the data
                                    tweets_data_filtered['place_name']=row[0]
                                    tweets_data_filtered['place_address']=row[1]
                                    tweets_data_filtered['place_city']=row[2]
                                    tweets_data_filtered['place_state']=row[3]
                                    tweets_data_filtered['place_zip']=row[4]
                                    tweets_data_filtered['place_county']=row[5]
                                    tweets_data_filtered['place_latitude']=row[6]
                                    tweets_data_filtered['place_longitude']=row[7]
                                    tweets_data_filtered['place_feature']=row[8]
                                    tweets1=tweets1.append(tweets_data_filtered)      #append/store results to existing results
                                    counter=counter+1                                 #counter only for reference of result returned
                    except:
                        continue
            len_compiled=len(tweets1.index)                         #keep track of number of records found in the current opened file
            if len_compiled > 0:                                    #if at least one recor found  then extract specific attributes from data frame
                tweets = tweets1[['timestamp',u'text',u'id','user.location',u'user.screen_name',u'user.followers_count',u'user.friends_count',u'user.time_zone','place_name','place_address','place_city','place_state','place_county','place_latitude','place_longitude','place_feature']]
            print 'finishing file, now moving forward...'
            if (counter != 0):                                      #save records found in current file in another file named as in the file_name var
                with open(file_name, 'w') as fout:
                    fout.write(json.dumps(tweets.to_json(path_or_buf = None, orient = 'records'), indent=4)) #save in json format into file
            tweets_data_in_json=[]  #reset data frames, list, counters etc.
            tweets_data_filtered=[]
            tweets_data_filtered1=[]
            tweets = pd.DataFrame()
            tweets1 = pd.DataFrame()
            counter=0
            t1 = time.time()
            total = t1-t0           #display tota time consumed to complete a compressed raw json file
            print total

