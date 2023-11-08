#IMPORTING ALL THE REQUIRED LIBRARIES

import pymongo
import psycopg2 as pg2
import isodate
import pandas as pd
import googleapiclient.discovery
import streamlit as st
from streamlit_option_menu import option_menu

#YOUTUBE API KEY 

api_key = 'AIzaSyALuc2p82RJXkJBFaiTGretJGx79PXsKGM'
api_service_name = "youtube"
api_version = "v3"
    
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# CHANNEL DATA RETRIEVAL

def channel_data(channel_id):
        chan_req = youtube.channels().list(
            part="snippet,contentDetails,statistics,status",
            id = channel_id
        )
        chan_res = chan_req.execute()
    
        ch_info = dict(channel_name = chan_res['items'][0]['snippet']['title'],
                channel_id = chan_res['items'][0]['id'],
                subscribers = chan_res['items'][0]['statistics']['subscriberCount'],
                views = chan_res['items'][0]['statistics']['viewCount'],
                description = chan_res['items'][0]['snippet']['description'],
                status = chan_res['items'][0]['status']['privacyStatus'],
                playlist = chan_res['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
        #print (ch_info)
        return ch_info

#PLAYLIST DATA RETRIEVAL
def get_playlist_details(channel_id):
    
        chan_req = youtube.channels().list(id = channel_id,
                                              part = 'contentDetails')
    
        chan_res =chan_req.execute()
        playlist_id = chan_res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
        play = []
        next_page_token = None
    
        while 1:
            play_res = youtube.playlistItems().list(playlistId= playlist_id,
                                             part = 'snippet',
                                             maxResults = 50,
                                             pageToken = next_page_token).execute()
            for item in play_res['items']:
                        play_info = dict(Channel_Id =item['snippet']['channelId'],
                                         Playlist_Id =item['snippet']['playlistId'],
                                         Video_Id = item['snippet']['resourceId']['videoId'])
                        play.append(play_info)
            next_page_token = play_res.get('nextPageToken')
        
            if next_page_token is None:
                break
        return play

# FETCHING VIDEO IDS USING PLAYLIST UPLOADS DATA

def get_video_id(channel_id):
    
        chan_req = youtube.channels().list(id = channel_id,
                                              part = 'contentDetails')
    
        chan_res =chan_req.execute()
        playlist_id = chan_res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
        videos = []
        next_page_token = None
    
        while 1:
            play_res = youtube.playlistItems().list(playlistId= playlist_id,
                                             part = 'snippet',
                                             maxResults = 50,
                                             pageToken = next_page_token).execute()
            for i in range(len(play_res['items'])):
                        videos.append(play_res['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token = play_res.get('nextPageToken')
        
            if next_page_token is None:
                break
        return videos

#VIDEO DATA RETRIEVAL

def video_data(Id):
    
             
        video_req = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id = str(Id)
                        )
        
        video_res = video_req.execute()
                
        for video in video_res['items']:
                
                    v_info = dict(Video_id = video['id'],
                                      Video_name = video['snippet']['title'],
                                      Video_description = video['snippet']['description'],
                                      Tags = video['snippet'].get('tags'),
                                      PublishedAt = video['snippet']['publishedAt'],
                                      View_count = video['statistics']['viewCount'],
                                      Like_count = video['statistics'].get('likeCount'),
                                      Favourite_count =video['statistics']['favoriteCount'],
                                      Comment_count = video['statistics'].get('commentCount'),
                                      Duration_in_sec = video['contentDetails']['duration'],
                                      Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                      Caption_Status = video['contentDetails']['caption'],
                                      )
                    
                    
                    def duration_sec(dur = v_info['Duration_in_sec']):
                                d = isodate.parse_duration(dur).total_seconds()
                                return d
                    duration = duration_sec()

                    v_info['Duration_in_sec'] = int(duration)
        return v_info
                    
#COMMENTS DATA RETRIEVAL

def comments_data(Id):
        try:
            comments = []
            thread_req = youtube.commentThreads().list(
                    part="id,snippet",
                    videoId= str(Id),
                    maxResults = 20
                    )
        

            cm_res = thread_req.execute()
    
            for i,item in enumerate(cm_res['items']):
                    comment_info = dict(Video_id = cm_res['items'][i]['snippet']['videoId'],
                                    Comment_id = cm_res['items'][i]['id'],
                                    Comment_Text = cm_res['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    Comment_Author = cm_res['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    Comment_PublishedAt =cm_res['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'])
    
                    comments.append(comment_info)
        except:
            pass
        return comments    

# COMPLETE CHANNEL DATA

def complete_channel_data (channel_id):
                 
          if channel_id=="":
            st.error('Channel Not Entered')
          else:
            details= channel_data(channel_id)   #getting channel details and storing it in details
        
          
            playlist_details = get_playlist_details(channel_id) #playlist details saved here
        
          
            video_ids = get_video_id(channel_id) #getting the video ids and storing in video_ids
          
            videos = []                          #storing video details in videos for every video_id
                                            
            comments=[]                          #storing comment details for every video id
            for Id in video_ids:
                v= video_data(Id)
                videos.append(v)
                c= comments_data(Id)
                comments.append(c)
            my_data =dict(channel_details = details,
                        playlist_details = playlist_details,
                        video_details = videos,
                        Comments = comments  )
            return my_data

#BUILDING STREAMLIT UI
st.set_page_config(page_title = 'YouTube Data Harvesting',layout='wide')
st.markdown("<h1 style='text-align: center; color: black;'>YouTube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)


user_input = st.text_input("Enter a valid Channel id:")
channel_id = user_input
button = st.button("SUBMIT")
menu = option_menu(None,["Select Your Choice","Store in Mongodb","Migrate to SQL","SQL Queries"], orientation="horizontal")

#DISPLAYING THE DATA
if button:
    if channel_id != '':
        final_data = complete_channel_data(channel_id )       
        st.json(final_data)
    else:
          st.error("Kindly enter channel Id")


#MONGODB STORAGE
my_client = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = my_client['Youtube_data']
my_col = my_db['channel_data']
def mongo_store():

        chan_ids = my_col.distinct("channel_details.channel_id")
        final_data = complete_channel_data(channel_id )
        if final_data["channel_details"]["channel_id"] in chan_ids:
                st.error("Channel data already exists! Try a different Id.")
        else:
                my_col.insert_one(final_data)
                st.success("Storage successful!!")        

if menu == "Store in Mongodb":
        if channel_id != '':
                button2 = st.button("Store the data")
                if button2:
                        mongo_store()                  
        else:
                st.error("Kindly enter channel Id")  

#FETCHING DATA FROM MONGODB FOR MIGRATION
d1 = []
for x in my_col.find({"channel_details.channel_id":user_input},{"_id":0,"channel_details":1}): 
       d1.append(x)
d2 = []

for x in my_col.find({"channel_details.channel_id":user_input},{"_id":0,"video_details":1}): 
       d2.append(x)
       
d3 = []
for x in my_col.find({"channel_details.channel_id":user_input},{"_id":0,"playlist_details":1,}): 
       d3.append(x)
d4 = []
for x in my_col.find({"channel_details.channel_id":user_input},{"_id":0,"Comments":1,}): 
       d4.append(x)


#DATA INSERTION INTO SQL TABLES

#Channel table
def channel_table():
        for i,item in enumerate(d1):
                
                insert = '''INSERT INTO channel(channel_name, channel_id, subscribers, views, description, status)VALUES(%s,%s,%s,%s,%s,%s)'''
                values=(d1[i]['channel_details']['channel_name'],
                        d1[i]['channel_details']['channel_id'],
                        d1[i]['channel_details']['subscribers'],
                        d1[i]['channel_details']['views'],
                        d1[i]['channel_details']['description'],
                        d1[i]['channel_details']['status'])
        
                cur.execute(insert,values)
                conn.commit()
  
#Video table
def video_table():
        for i,item in enumerate(d2):
                
                for item in d2[i]['video_details']:

                        insert = '''INSERT INTO videos(video_id, video_name, video_description, tags, publishedAt, view_count, like_count, favourite_count, comment_count, duration_in_sec, thumbnail, caption_status)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

                        values=(item['Video_id'],
                                item['Video_name'],
                                item['Video_description'],
                                item['Tags'],
                                item['PublishedAt'],
                                item['View_count'],
                                item['Like_count'],
                                item['Favourite_count'],
                                item['Comment_count'],
                                item['Duration_in_sec'],
                                item['Thumbnail'],
                                item['Caption_Status'])

               
                        cur.execute(insert,values)
                        conn.commit()
                
#Playlist table

def playlist_table():
    for i,item in enumerate(d3):
            
            for item in d3[i]['playlist_details']:

                insert = '''INSERT INTO playlist(channel_id, playlist_id, video_id)VALUES(%s,%s,%s)'''
                values=(item['Channel_Id'],
                        item['Playlist_Id'],
                        item['Video_Id'])
               

                cur.execute(insert,values)
                conn.commit()

#Comment Table

def comment_table():
    d = d4[0]['Comments']
    #print(d)
    for i,item in enumerate(d):
            for item in d[i]:
                insert = '''INSERT INTO comments(comment_id,video_id,comment_text,comment_author,comment_publishedat)VALUES(%s,%s,%s,%s,%s)'''
                values= (item['Comment_id'],
                         item['Video_id'],
                         item['Comment_Text'],
                         item['Comment_Author'],
                         item['Comment_PublishedAt'])

                #try:
                cur.execute(insert,values)
                conn.commit()
                #except Exception as e:
                       #print(e)
#SQL DATA MIGRATION
if menu == "Migrate to SQL":
        if channel_id != '':
                
                button3 = st.button("Start Migration")
                if button3:
                        conn = pg2.connect(database = 'Youtube_info',user = 'postgres',password= 'SU',port=5433)
                        cur = conn.cursor()
                        try:
                                def sql_tables():
                                        channel_table()
                                        video_table()
                                        playlist_table()
                                        comment_table()
                                        st.success("Data Migration Successful!!!")
                                sql_tables()    
                        except:
                                conn.rollback()
                                st.error("Channel data already exists!!!")
        else:
               st.error("Kindly enter channel Id")

# SQL QUERIES
if menu == "SQL Queries":
       
                conn = pg2.connect(database = 'Youtube_info',user = 'postgres',password= 'SU',port=5433)
                cur = conn.cursor()
                option = st.selectbox("Queries:",
                        ("None",
                        "1. What are the names of all the videos and their corresponding channels?",
                        "2. Which channels have the most number of videos, and how many videos do they have?", 
                        "3. What are the top 10 most viewed videos and their respective channels?",
                        "4. How many comments were made on each video, and what are their corresponding video names?",
                        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                        "6. What is the total number of likes for each video, and what are their corresponding video names?",
                        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                        "8. What are the names of all the channels that have published videos in the year 2022?",
                        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                        "10. Which videos have the highest number of comments, and what are their corresponding channel names?"),
                        index=None,
                        placeholder="Select a query...")

                st.write('You selected:', option)
# QUESTION 1

                if option == "1. What are the names of all the videos and their corresponding channels?":
                        cur.execute('''SELECT video_name,channel.channel_name FROM videos 
                                        JOIN playlist 
                                        ON playlist.video_id = videos.video_id 
                                        JOIN channel
                                ON playlist.channel_id = channel.channel_id''')
                        result1 = cur.fetchall()
                        df1 = pd.DataFrame(result1,columns = ['Video_name',"Channel_name"])
                        st.dataframe(df1)
# QUESTION 2

                elif option == "2. Which channels have the most number of videos, and how many videos do they have?":
                        cur.execute('''select channel.channel_name,count(video_id) from playlist
                                        join channel
                                        on channel.channel_id = playlist.channel_id
                                        group by channel_name   
                                        order by count(video_id) desc
                                        limit (5)''')

                        result2 =  cur.fetchall()
                        df2 = pd.DataFrame(result2,columns = ['Channel_name',"Video_count"])
                        st.dataframe(df2)
# QUESTION 3

                elif option == "3. What are the top 10 most viewed videos and their respective channels?":
                        cur.execute('''select channel.channel_name,video_name,view_count from videos
                                        join playlist
                                        on playlist.video_id = videos.video_id
                                        join channel 
                                        on playlist.channel_id = channel.channel_id
                                        order by view_count desc
                                        limit(10)''')
                        result3 = cur.fetchall()
                        df3 = pd.DataFrame(result3,columns = ['channel_name','video_name',"view_count"])
                        st.dataframe(df3)
# QUESTION 4
                
                elif option == "4. How many comments were made on each video, and what are their corresponding video names?":
                       cur.execute('''select video_name,comment_count from videos''')
                       result4 = cur.fetchall()
                       df4 = pd.DataFrame(result4,columns = ["video_name","comment_count"])
                       st.dataframe(df4)

# QUESTION 5        

                elif option == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
                        cur.execute('''select channel.channel_name,video_name,like_count from videos
                                        join playlist
                                        on playlist.video_id = videos.video_id
                                        join channel
                                        on channel.channel_id = playlist.channel_id
                                        where like_count is not null
                                        order by like_count desc
                                        limit (10)''')
                        result5 = cur.fetchall()
                        df5 = pd.DataFrame(result5,columns = ["channel_name","video_name","likes_count"])
                        st.dataframe(df5)
# QUESTION 6
                
                elif option == "6. What is the total number of likes for each video, and what are their corresponding video names?":
                        cur.execute('''select video_name,like_count from videos''')
                        result6 = cur.fetchall()
                        df6 = pd.DataFrame(result6,columns = ["video_name","like_count"])
                        st.dataframe(df6)
# QUESTION 7

                elif option == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
                        cur.execute('''select channel_name,views from channel''')
                        result7 = cur.fetchall()
                        df7 = pd.DataFrame(result7,columns = ["channel_name","Total_views"])
                        st.dataframe(df7)
# QUESTION 8

                elif option == "8. What are the names of all the channels that have published videos in the year 2022?":
                        cur.execute('''select channel.channel_name,video_name,publishedat from videos
                                        join playlist
                                        on playlist.video_id = videos.video_id
                                        join channel
                                        on channel.channel_id = playlist.channel_id
                                        where publishedat between '2022-01-01' and '2023-01-01' ''')
                        result8 = cur.fetchall()
                        df8 = pd.DataFrame(result8,columns = ["channel_name","video_name","published_2022"])
                        st.dataframe(df8)
# QUESTION 9
                elif option == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
                        cur.execute('''select channel.channel_name,AVG(videos.duration_in_sec) from videos
                                        join playlist
                                        on playlist.video_id = videos.video_id
                                        join channel
                                        on channel.channel_id = playlist.channel_id
                                        group by channel_name ''')
                        result9= cur.fetchall()
                        df9 = pd.DataFrame(result9,columns = ["channel_name","avg_videoduration_in_seconds"])
                        st.dataframe(df9)
# QUESTION 10
                elif option == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
                        cur.execute('''select channel.channel_name,video_name,comment_count from videos
                                        join playlist
                                        on videos.video_id = playlist.video_id
                                        join channel
                                        on channel.channel_id = playlist.channel_id
                                        where comment_count is not null
                                        order by comment_count desc
                                        limit (5) ''')
                        result10= cur.fetchall()
                        df10 = pd.DataFrame(result10,columns = ["channel_name","video_name","comment_count"])
                        st.dataframe(df10)
                




                        
     

     
