from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

def APi_connect():
    Api_Id = 'AIzaSyCI8yMCeC3FQFEZ2GFearr7elnoeXjdqp4'
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build('youtube', 'v3', developerKey=Api_Id)
    return youtube
youtube = APi_connect()

#get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data


#get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data


#get_playlist_details

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data


client=pymongo.MongoClient('mongodb://localhost:27017/')
db=client["YouTube_Harvest_Update"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"

# Channel information 

def channel_table():
    try:
        with psycopg2.connect(
            host="localhost",
            user="postgres",
            password="admin",
            database="Youtube_data_Database",
            port="5432"
        ) as mydb:
            with mydb.cursor() as cursor:
                create_query = '''CREATE TABLE IF NOT EXISTS channel (
                                    Channel_Name varchar(100),
                                    Channel_Id varchar(100) PRIMARY KEY,
                                    Subscribers bigint,
                                    Views bigint,
                                    Total_Videos int,
                                    Channel_Description text,
                                    Playlist_Id varchar(100)
                                )'''
                cursor.execute(create_query)
                mydb.commit()

                ch_list = []

                # Establish MongoDB connection
                client = MongoClient('mongodb://localhost:27017/')
                db = client["YouTube_Harvest_Update"]
                collection1 = db["channel_details"]

                for ch_data in collection1.find({}, {"_id": 0, "channel_information": 1}):
                    ch_list.append(ch_data["channel_information"])

                df1 = pd.DataFrame(ch_list)

                for index, row in df1.iterrows():
                    insert_query = '''INSERT INTO channel (Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)'''
                    values = (row['Channel_Name'], row['Channel_Id'], row['Subscribers'],
                              row['Views'], row['Total_Videos'], row['Channel_Description'], row['Playlist_Id'])

                    try:
                        cursor.execute(insert_query, values)
                        mydb.commit()
                        print(f"Inserted data for Channel_Id: {row['Channel_Id']} successfully")
                    except Exception as e:
                        print(f"Error inserting data: {e}")

    except Exception as e:
        print(f"Error: {e}")

# Call the function to create the table and insert data
channel_table()


# Playlist Information

import psycopg2
import pandas as pd
from pymongo import MongoClient

def playlist_table():
    try:
        # Establish PostgreSQL connection
        mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="admin",
            database="Youtube_data_Database",
            port="5432"
        )
        cursor = mydb.cursor()

        # Drop table if exists
        drop_query = '''DROP TABLE IF EXISTS playlists'''
        cursor.execute(drop_query)
        mydb.commit()

        # Create table if not exists
        create_query = '''CREATE TABLE IF NOT EXISTS playlists (
                            Playlist_Id varchar(100) PRIMARY KEY,
                            Title varchar(100),
                            Channel_Id varchar(100),
                            Channel_Name varchar(100),
                            PublishedAt timestamp,
                            Video_Count int
                        )'''
        cursor.execute(create_query)
        mydb.commit()

        # Fetch data from MongoDB
        pl_list = []
        client = MongoClient('mongodb://localhost:27017/')
        db = client["YouTube_Harvest_Update"]
        collection1 = db["channel_details"]

        for pl_data in collection1.find({}, {"_id": 0, "playlist_information": 1}):
            for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])

        df1 = pd.DataFrame(pl_list)

        # Insert data into PostgreSQL table
        for index, row in df1.iterrows():
            insert_query = ''' INSERT INTO playlists(
                                Playlist_Id,
                                Title,
                                Channel_Id,
                                Channel_Name,
                                PublishedAt,
                                Video_Count)
                                VALUES (%s, %s, %s, %s, %s, %s)'''
            values = (row['Playlist_Id'],
                      row['Title'],
                      row['Channel_Id'],
                      row['Channel_Name'],
                      row['PublishedAt'],
                      row['Video_Count']
                      )
            cursor.execute(insert_query, values)
            mydb.commit()

        print("Playlist table created and data inserted successfully")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the database connection
        if mydb:
            mydb.close()

# Call the function to create the table and insert data
playlist_table()


#Video_Information

import psycopg2
import pandas as pd
from pymongo import MongoClient

def videos_table():
    try:
        # Establish PostgreSQL connection
        mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="admin",
            database="Youtube_data_Database",
            port="5432"
        )
        cursor = mydb.cursor()

        # Drop table if exists
        drop_query = '''DROP TABLE IF EXISTS videos'''
        cursor.execute(drop_query)
        mydb.commit()

        # Create table if not exists
        create_query = '''CREATE TABLE IF NOT EXISTS videos (
                            Channel_Name varchar(100),
                            Channel_Id varchar(100),
                            Video_Id varchar(100),
                            Title varchar(200),
                            Tags text,
                            Thumbnail varchar(200),
                            Description text,
                            Published_Date timestamp,
                            Duration interval,
                            Views bigint,
                            Likes bigint,
                            Comments int,
                            Favorite_Count int,
                            Definition varchar(20),
                            Caption_Status varchar(100)
                        )'''
        cursor.execute(create_query)
        mydb.commit()

        # Fetch data from MongoDB
        vi_list = []
        client = MongoClient('mongodb://localhost:27017/')
        db = client["YouTube_Harvest_Update"]
        collection1 = db["channel_details"]

        for vi_data in collection1.find({}, {"_id": 0, "video_information": 1}):
            for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])

        df1 = pd.DataFrame(vi_list)

        # Insert data into PostgreSQL table
        for index, row in df1.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                       Channel_Id,
                                       Video_Id,  
                                       Title,
                                       Tags,
                                       Thumbnail,
                                       Description,
                                       Published_Date,
                                       Duration,
                                       Views,
                                       Likes,
                                       Comments,
                                       Favorite_Count,
                                       Definition,
                                       Caption_Status )
                                       
                                       values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
   


            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status']
                    )

            cursor.execute(insert_query, values)
            mydb.commit()

        print("Videos table created and data inserted successfully")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the database connection
        if mydb:
            mydb.close()

# Call the function to create the table and insert data
videos_table()



#Comment_information


import psycopg2
import pandas as pd
from pymongo import MongoClient

def comments_table():
    try:
        # Establish PostgreSQL connection
        mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="admin",
            database="Youtube_data_Database",
            port="5432"
        )
        cursor = mydb.cursor()

        # Drop table if exists
        drop_query = '''DROP TABLE IF EXISTS comments'''
        cursor.execute(drop_query)
        mydb.commit()

        # Create table if not exists
        create_query = '''CREATE TABLE IF NOT EXISTS comments (
                          Comment_Id varchar(100) primary key,
                          Video_Id varchar(100) ,
                          Comment_Text text,
                          Comment_Author varchar(100) ,
                          Comment_Published timestamp
                        )'''
        cursor.execute(create_query)
        mydb.commit()

        # Fetch data from MongoDB
        com_list = []
        client = MongoClient('mongodb://localhost:27017/')
        db = client["YouTube_Harvest_Update"]
        collection1 = db["channel_details"]

        for com_data in collection1.find({}, {"_id": 0, "comment_information": 1}):
            for i in range(len(com_data["comment_information"])):
                com_list.append(com_data["comment_information"][i])

        df1 = pd.DataFrame(com_list)

        # Insert data into PostgreSQL table
        for index, row in df1.iterrows():
            select_query = "SELECT Comment_Id FROM comments WHERE Comment_Id = %s"
            cursor.execute(select_query, (row['Comment_Id'],))
            existing_record = cursor.fetchone()

            if existing_record:
                # Handle duplicate: You may choose to skip or update the record
                print(f"Skipping duplicate Comment_Id: {row['Comment_Id']}")
            else:
                insert_query = '''
                INSERT INTO comments(Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                VALUES (%s, %s, %s, %s, %s)
                '''
                values = (row['Comment_Id'], row['Video_Id'], row['Comment_Text'], row['Comment_Author'], row['Comment_Published'])

                cursor.execute(insert_query, values)
                mydb.commit()

                print(f"Inserted Comment_Id: {row['Comment_Id']} successfully")

        # Move these lines inside the for loop
        print("Comments table created and data inserted successfully")

    finally:
        # Close the database connection
        if mydb:
            mydb.close()

# Call the function to create the table and insert data
comments_table()


def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables are created successfully"

def show_channels_table():
    ch_list=[]
    db=client["YouTube_Harvest_Update"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
            ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    return df

def show_playlists_table():
    pl_list=[]
    db=client["YouTube_Harvest_Update"]
    collection1=db["channel_details"]
    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
                pl_list.append(pl_data["playlist_information"])
    df1=pd.DataFrame(pl_list)

    return df1


def show_videos_table():
    vi_list=[]
    db=client["YouTube_Harvest_Update"]
    collection1=db["channel_details"]
    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
                vi_list.append(ch_data["video_information"])
    df2=pd.DataFrame(vi_list)

    return df2

def show_comments_table():
    com_list=[]
    db=client["YouTube_Harvest_Update"]
    collection1=db["channel_details"]
    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
                com_list.append(ch_data["comment_information"])
    df3=pd.DataFrame(com_list)

    return df3

# Streamlit 

with st.sidebar:
    st.title(":red[YouTube Data Harvesting and Warehousing]")
    
channel_id=st.text_input("Enter The Channel ID ")

if st.button("collect and store data"):
    ch_ids=[]
    db = client["YouTube_Harvest_Update"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({}, {"_id": 0, "channel_information": 1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])


    if channel_id in ch_ids:
        st.success("Channel Details Already Exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)    


if st.button("Migrate to SQL"):
    Tables=tables()
    st.success(Tables)

show_table=st.radio("SELECT THE TABLE FOR VIEW" ,("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table() 

elif show_table=="VIDEOS":
    show_videos_table() 

elif show_table=="COMMENTS":
    show_comments_table()     


# Database connection

mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="admin",
            database="Youtube_data_Database",
            port="5432"
        )
cursor = mydb.cursor()

question=st.selectbox("Select Your Question", ("1. All the videos and channel name",
                                               "2. channels with most number of videos",
                                               "3. 10 most viewed videos",
                                               "4. comments in each videos",
                                               "5. videos with highest likes",
                                               "6. likes of all videos",
                                               "7. views of each channel",
                                               "8. videos published in the year 2023",
                                               "9. average duration of all videos in each channel",
                                               "10.videos with highest number of comments"))