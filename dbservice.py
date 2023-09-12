import streamlit as st
import pymongo
import psycopg2
import pandas as pd
import time


# -------------------- Mongo DB - Start -------------------
# store data from temp collection to project database in Mongo DB
def store_mongodb(channel_name, database):
    mdbconn = pymongo.MongoClient("mongodb+srv://sureshkumar:sureshkumar@sureshkumar.elrndfv.mongodb.net/?retryWrites=true&w=majority")
    db = mdbconn['temp_youtube_data']
    col = db.list_collection_names()

    if len(col) == 0:
        st.info("There is no data retrived from YouTube")
    else:
        data_youtube = {}
        mdbconn = pymongo.MongoClient("mongodb+srv://sureshkumar:sureshkumar@sureshkumar.elrndfv.mongodb.net/?retryWrites=true&w=majority")
    
        db = mdbconn['temp_youtube_data']
        col = db.list_collection_names()
        channel_name = col[0]

        col1 = db[channel_name]
        for i in col1.find():
            data_youtube.update(i)
        
        list_collections_name = list_mongodb_collection_names(database)

        if channel_name not in list_collections_name:
            store_collection(channel_name, database, data_youtube)

            st.balloons()

            n_successalert = st.success("The data has been successfully stored in the MongoDB database")
            time.sleep(3) # Wait for 3 seconds
            n_successalert.empty()

            drop_temp_collection()
        else:
            mdbconn = pymongo.MongoClient("mongodb+srv://sureshkumar:sureshkumar@sureshkumar.elrndfv.mongodb.net/?retryWrites=true&w=majority")
            db = mdbconn[database]
            db[channel_name].drop()
            store_collection(channel_name, database, data_youtube)

            st.balloons()
            o_successalert = st.success("The data has been successfully overwritten and updated in MongoDB database")
            time.sleep(3) # Wait for 3 seconds
            o_successalert.empty()
                
            drop_temp_collection()


# list of all collections in the 'project_youtube' database in MongoDB
def list_mongodb_collection_names(database):
    mdbconn = pymongo.MongoClient("mongodb+srv://sureshkumar:sureshkumar@sureshkumar.elrndfv.mongodb.net/?retryWrites=true&w=majority")
    db = mdbconn[database]
    col = db.list_collection_names()
    col.sort(reverse=False)
    return col

# collection names in order wise
def order_mongodb_collection_names(database):
    m = list_mongodb_collection_names(database)
    if m == []:
        st.info("The Mongodb database is currently empty")
    else:
        st.subheader('List of collections in MongoDB database')
        c = 1
        m = list_mongodb_collection_names(database)
        for i in m:
            st.write(str(c) + ' - ' + i)
            c += 1
        st.markdown("""---""")

# retrive data store in temp collection of MongoDB
def store_collection(channel_name, database, data_youtube):
    mdbconn = pymongo.MongoClient(
        "mongodb+srv://sureshkumar:sureshkumar@sureshkumar.elrndfv.mongodb.net/?retryWrites=true&w=majority")
    db = mdbconn[database]
    col = db[channel_name]
    col.insert_one(data_youtube)

# temporary collection to store and retrive data and finally automatically drop
def drop_temp_collection():
    mdbconn = pymongo.MongoClient(
        "mongodb+srv://sureshkumar:sureshkumar@sureshkumar.elrndfv.mongodb.net/?retryWrites=true&w=majority")
    db = mdbconn['temp_youtube_data']
    col = db.list_collection_names()
    if len(col) > 0:
        for i in col:
            db.drop_collection(i)

# -------------------- Mongo DB - End -------------------


# -------------------- SQL DB - Start -------------------
def sql_create_tables():
    sqlconn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='root123', database='youtube_data')
    cursor = sqlconn.cursor()
    cursor.execute("create table if not exists channel(channel_id varchar(255) primary key,\
                                        channel_name varchar(255),\
                                        subscription_count int,\
                                        channel_views int,\
                                        channel_description text,\
                                        channel_videos int,\
                                        upload_id varchar(255),\
                                        country varchar(255),\
                                        thumbnail_image varchar(255),\
                                        created_date date,\
                                        channel_link varchar(255))")
    
    cursor.execute("create table if not exists playlist(\
                                        playlist_id varchar(255) primary key,\
	                                    playlist_name varchar(255),\
	                                    channel_id varchar(255),\
	                                    upload_id varchar(255))")
    
    cursor.execute("create table if not exists video(\
                                        video_id varchar(255) primary key,\
	                                    video_name varchar(255),\
	                                    video_description text,\
	                                    upload_id varchar(255),\
                                        tags text,\
                                        published_date date,\
                                        published_time time,\
                                        view_count int,\
                                        like_count int,\
                                        favourite_count int,\
                                        comment_count int,\
                                        duration time,\
                                        thumbnail varchar(255),\
                                        caption_status varchar(255))")
    
    cursor.execute("create table if not exists comment(\
                                        comment_id varchar(255) primary key,\
	                                    comment_text text,\
	                                    comment_author varchar(255),\
	                                    comment_published_date date,\
                                        comment_published_time time,\
                                        video_id varchar(255))")
    sqlconn.commit()

# SQL channel names list
def list_sql_channel_names():
    sqlconn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='root123', database='youtube_data')
    cursor = sqlconn.cursor()
    cursor.execute("select channel_name from channel")
    s = cursor.fetchall()
    s = [i[0] for i in s]
    s.sort(reverse=False)
    return s

# display the all channel names from SQL channel table
def order_sql_channel_names():
    s = list_sql_channel_names()
    if s == []:
        st.info("The SQL database is currently empty")
    else:
        st.subheader("List of channels in SQL database")
        c = 1
        for i in s:
            st.write(str(c) + ' - ' + i)
            c += 1

# data migrating to channel table
def sql_channel(database, col_input):
    mdbconn = pymongo.MongoClient("mongodb+srv://sureshkumar:sureshkumar@sureshkumar.elrndfv.mongodb.net/?retryWrites=true&w=majority")
    db = mdbconn[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data = []
    for i in col.find({}, {'_id': 0, 'channel_name': 1}):
        data.append(i['channel_name'])

    channel = pd.DataFrame(data)

    channel = channel.reindex(columns=['channel_id', 'channel_name', 'subscription_count', 'channel_views', 'channel_description', 'channel_videos', 'upload_id', 'country', 'thumbnail_image', 'created_date', 'channel_link'])
    channel['subscription_count'] = pd.to_numeric(channel['subscription_count'])
    channel['channel_views'] = pd.to_numeric(channel['channel_views'])

    return channel

# data migrating to playlist table
def sql_playlists(database, col_input):
    mdbconn = pymongo.MongoClient("mongodb+srv://sureshkumar:sureshkumar@sureshkumar.elrndfv.mongodb.net/?retryWrites=true&w=majority")
    db = mdbconn[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data = []

    for i in col.find({}, {'_id': 0, 'playlists': 1}):
        data.append(i['playlists'].values())

    playlists = pd.DataFrame(data[0])
    playlists = playlists.reindex(columns=['playlist_id', 'playlist_name', 'channel_id', 'upload_id'])

    return playlists

def sql_videos(database, col_input):
    mdbconn = pymongo.MongoClient("mongodb+srv://sureshkumar:sureshkumar@sureshkumar.elrndfv.mongodb.net/?retryWrites=true&w=majority")
    db = mdbconn[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data = []
    for i in col.find({}, {'_id': 0, 'channel_name': 0, 'playlists': 0}):
        data.append(i.values())

    videos = pd.DataFrame(data[0])
    videos = videos.reindex(columns=['video_id', 'video_name', 'video_description', 'upload_id', 'tags', 'published_date', 'published_time',
                'view_count', 'like_count', 'favourite_count', 'comment_count', 'duration', 'thumbnail',
                'caption_status', 'comments'])

    videos['published_date'] = pd.to_datetime(videos['published_date']).dt.date
    videos['published_time'] = pd.to_datetime(videos['published_time'], format='%H:%M:%S').dt.time
    videos['view_count'] = pd.to_numeric(videos['view_count'])
    videos['like_count'] = pd.to_numeric(videos['like_count'])
    videos['favourite_count'] = pd.to_numeric(videos['favourite_count'])
    videos['comment_count'] = pd.to_numeric(videos['comment_count'])
    videos['duration'] = pd.to_datetime(videos['duration'], format='%H:%M:%S').dt.time
    videos.drop(columns='comments', inplace=True)

    return videos

# data migrating to comment table
def sql_comments(database, col_input):
    mdbconn = pymongo.MongoClient("mongodb+srv://sureshkumar:sureshkumar@sureshkumar.elrndfv.mongodb.net/?retryWrites=true&w=majority")
    db = mdbconn[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data_videos = []
    for i in col.find({}, {'_id': 0, 'channel_name': 0, 'playlists': 0}):
        data_videos.append(i.values())

    videos = pd.DataFrame(data_videos[0])
    videos = videos.reindex(columns=['video_id', 'video_name', 'video_description', 'upload_id', 'tags', 'published_date', 'published_time',
                'view_count', 'like_count', 'favourite_count', 'comment_count', 'duration', 'thumbnail',
                'caption_status', 'comments'])

    videos['published_date'] = pd.to_datetime(videos['published_date']).dt.date
    videos['published_time'] = pd.to_datetime(videos['published_time'], format='%H:%M:%S').dt.time
    videos['view_count'] = pd.to_numeric(videos['view_count'])
    videos['like_count'] = pd.to_numeric(videos['like_count'])
    videos['favourite_count'] = pd.to_numeric(videos['favourite_count'])
    videos['comment_count'] = pd.to_numeric(videos['comment_count'])
    videos['duration'] = pd.to_datetime(videos['duration'], format='%H:%M:%S').dt.time

    data = []
    for i in videos['comments'].tolist():
        if isinstance(i, dict):
            data.extend(list(i.values()))
        else:
            pass

    comments = pd.DataFrame(data)
    comments = comments.reindex(columns=['comment_id', 'comment_text', 'comment_author', 'comment_published_date', 'comment_published_time', 'video_id'])
    comments['comment_published_date'] = pd.to_datetime(comments['comment_published_date']).dt.date
    comments['comment_published_time'] = pd.to_datetime(comments['comment_published_time'], format='%H:%M:%S').dt.time

    return comments

def store_sql(database):
    sql_create_tables()

    m = list_mongodb_collection_names(database)
    s = list_sql_channel_names()

    if m==s==[]:
        st.info("Both Mongodb and SQL databases are currently empty")
    else:
        order_mongodb_collection_names(database)
        order_sql_channel_names()

        list_mongodb_notin_sql = ['Please Select']

        m = list_mongodb_collection_names(database)
        s = list_sql_channel_names()

        for i in m:
            if i not in s:
                list_mongodb_notin_sql.append(i)       

        option_sql = st.selectbox('Select Channel Name for Migrate to SQL', list_mongodb_notin_sql)

        if option_sql:
            if option_sql == 'Please Select':
                st.warning('Please select the channel')
            else:
                option = option_sql

                pd.set_option('display.max_rows', None)
                pd.set_option('display.max_columns', None)

                channel = sql_channel(database, option)
                playlists = sql_playlists(database, option)
                videos = sql_videos(database, option)
                comments = sql_comments(database, option)

                sqlconn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='root123', database='youtube_data')
                cursor = sqlconn.cursor()

                cursor.executemany("insert into channel(channel_id, channel_name, subscription_count, channel_views, channel_description, channel_videos, upload_id, country, thumbnail_image, created_date, channel_link) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                                            channel.values.tolist())

                cursor.executemany("insert into playlist(playlist_id, playlist_name, channel_id, upload_id) values(%s,%s,%s,%s)", 
                                            playlists.values.tolist())

                cursor.executemany("insert into video(video_id, video_name, video_description, upload_id, tags, published_date, published_time, view_count, like_count, favourite_count, comment_count, duration, thumbnail, caption_status) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                            videos.values.tolist())
                
                cursor.executemany("insert into comment(comment_id, comment_text, comment_author, comment_published_date, comment_published_time, video_id) values (%s, %s, %s, %s, %s, %s)",
                                            comments.values.tolist())
                sqlconn.commit()

                st.balloons()
                
                sql_successalert = st.success("Migrated Data Successfully to SQL Data Warehouse")
                time.sleep(3) # Wait for 3 seconds
                sql_successalert.empty()

                sqlconn.close()


# -------------------- SQL Qureies - Start --------------------
def q1_allvideoname_channelname():
    q1_sqlconn = psycopg2.connect(host='localhost', user='postgres', password='root123', database='youtube_data')
    cursor = q1_sqlconn.cursor()
    cursor.execute('select video.video_name, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_id\
                    order by channel.channel_name ASC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Names', 'Channel Names'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data

def q2_channelname_totalvideos():
    q2_sqlconn = psycopg2.connect(host='localhost', user='postgres', password='root123', database='youtube_data')
    cursor = q2_sqlconn.cursor()
    cursor.execute("select distinct channel.channel_name, count(distinct video.video_id) as total\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by channel.channel_id\
                    order by total DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Channel Names', 'Total Videos'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data

def q3_mostviewvideos_channelname():
    q3_sqlconn = psycopg2.connect(host='localhost', user='postgres', password='root123', database='youtube_data')
    cursor = q3_sqlconn.cursor()
    cursor.execute("select distinct video.video_name, video.view_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    order by video.view_count DESC\
                    limit 10")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Names', 'Total Views', 'Channel Names'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data

def q4_videonames_totalcomments():
    q4_sqlconn = psycopg2.connect(host='localhost', user='postgres', password='root123', database='youtube_data')
    cursor = q4_sqlconn.cursor()
    cursor.execute('select video.video_name, video.comment_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_name\
                    order by video.comment_count DESC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Names', 'Total Comments', 'Channel Names'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data

def q5_videonames_highestlikes_channelname():
    q5_sqlconn = psycopg2.connect(host='localhost', user='postgres', password='root123', database='youtube_data')
    cursor = q5_sqlconn.cursor()
    cursor.execute("select distinct video.video_name, channel.channel_name, video.like_count\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    where video.like_count = (select max(like_count) from video)")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Name', 'Channel Name', 'Most Likes'], index=i)
    data = data.reindex(columns=['Video Name', 'Most Likes', 'Channel Name'])
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data

def q6_videonames_totallikes_channelname():
    q6_sqlconn = psycopg2.connect(host='localhost', user='postgres', password='root123', database='youtube_data')
    cursor = q6_sqlconn.cursor()
    cursor.execute("select distinct video.video_name, video.like_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_id\
                    order by video.like_count DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Names', 'Total Likes', 'Channel Names'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data

def q7_channelnames_totalviews():
    q7_sqlconn = psycopg2.connect(host='localhost', user='postgres', password='root123', database='youtube_data')
    cursor = q7_sqlconn.cursor()
    cursor.execute("select channel_name, channel_views from channel\
                    order by channel_views DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    data = pd.DataFrame(s, columns=['Channel Names', 'Total Views'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data

def q8_channelnames_releasevideos(year):
    q8_sqlconn = psycopg2.connect(host='localhost', user='postgres', password='root123', database='youtube_data')
    cursor = q8_sqlconn.cursor()
    cursor.execute('select distinct channel.channel_name, count(distinct video.video_id) as total\
        from video\
        inner join playlist on playlist.upload_id = video.upload_id\
        inner join channel on channel.channel_id = playlist.channel_id\
        where extract(year from video.published_date) = \'' + str(year) + '\'\
        group by channel.channel_id\
        order by total DESC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    data = pd.DataFrame(s, columns=['Channel Names', 'Published Videos'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data

def q9_channelnames_avgvideoduration():
    q9_sqlconn = psycopg2.connect(host='localhost', user='postgres', password='root123', database='youtube_data')
    cursor = q9_sqlconn.cursor()
    cursor.execute("select channel.channel_name, substring(cast(avg(video.duration) as varchar), 1, 8) as average\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by channel.channel_id\
                    order by average DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    data = pd.DataFrame(s, columns=['Channel Names', 'Average Video Duration'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data

def q10_videonames_channelnames_mostcomments():
    q10_sqlconn = psycopg2.connect(host='localhost', user='postgres', password='root123', database='youtube_data')
    cursor = q10_sqlconn.cursor()
    cursor.execute('select video.video_name, video.comment_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_name\
                    order by video.comment_count DESC\
                    limit 1')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Name', 'Total Comments', 'Channel Name'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data
# -------------------- SQL Qureies - End --------------------


def sql_qureries(database):
    st.subheader('Select the Query below')
    q1 = 'Q1-What are the names of all the videos and their corresponding channels?'
    q2 = 'Q2-Which channels have the most number of videos, and how many videos do they have?'
    q3 = 'Q3-What are the top 10 most viewed videos and their respective channels?'
    q4 = 'Q4-How many comments were made on each video with their corresponding video names?'
    q5 = 'Q5-Which video have the highest number of likes with their corresponding channel name?'
    q6 = 'Q6-What is the total number of likes for each video with their corresponding video names?'
    q7 = 'Q7-What is the total number of views for each channel with their corresponding channel names?'
    q8 = 'Q8-What are the names of all the channels that have published videos in the year 2022?'
    q9 = 'Q9-What is the average duration of all videos in each channel with corresponding channel names?'
    q10 = 'Q10-Which video have the highest number of comments with their corresponding channel name?'

    query_option = st.selectbox('Questions', ['Select One', q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])

    if query_option == q1:
        st.dataframe(q1_allvideoname_channelname())
    elif query_option == q2:
        st.dataframe(q2_channelname_totalvideos())
    elif query_option == q3:
        st.dataframe(q3_mostviewvideos_channelname())
    elif query_option == q4:
        st.dataframe(q4_videonames_totalcomments())
    elif query_option == q5:
        st.dataframe(q5_videonames_highestlikes_channelname())
    elif query_option == q6:
        st.dataframe(q6_videonames_totallikes_channelname())
    elif query_option == q7:
        st.dataframe(q7_channelnames_totalviews())
    elif query_option == q8:
        st.dataframe(q8_channelnames_releasevideos(2022))
    elif query_option == q9:
        st.dataframe(q9_channelnames_avgvideoduration())
    elif query_option == q10:
        st.dataframe(q10_videonames_channelnames_mostcomments())


# -------------------- SQL DB - End -------------------
