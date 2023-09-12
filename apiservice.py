from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

# Function to get Channel Details
def get_channel_details(youtube,get_channel_id):
  try:
    request = youtube.channels().list(part='snippet,contentDetails,statistics',id=get_channel_id)
    response = request.execute()
    data = dict(channel_id=response['items'][0]['id'],
                channel_name=response['items'][0]['snippet']['title'],                
                subscription_count=response['items'][0]['statistics']['subscriberCount'],
                channel_views=response['items'][0]['statistics']['viewCount'],
                channel_description=response['items'][0]['snippet']['description'],
                channel_videos=response['items'][0]['statistics']['videoCount'],                
                upload_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                country=response['items'][0]['snippet']['country'],
                thumbnail_image=response['items'][0]['snippet']['thumbnails']['medium']['url'],
                created_date=response['items'][0]['snippet']['publishedAt'][0:10],
                channel_link="https://www.youtube.com/channel/" + response['items'][0]['id'])
  except HttpError as e:
    error_message = e.content.decode("utf-8")
    print("An error occurred:", error_message)
  return data


# Funcion to get Playlist Details
def get_playlists_details(youtube, get_channel_id, get_upload_id):
  try:
    request = youtube.playlists().list(part="snippet,contentDetails,status", channelId=get_channel_id, maxResults=50)
    response = request.execute()

    playlists = {}
    ls = 1

    for i in range(0, len(response['items'])):
      data = {'playlist_id': response['items'][i]['id'], 
              'playlist_name': response['items'][i]['snippet']['title'], 
              'channel_id': get_channel_id, 
              'upload_id': get_upload_id}
      title = 'playlist_no_' + str(ls)
      playlists[title] = data
      ls += 1
    '''next_page_token = response.get('nextPageToken')
    morepages = True

    while morepages:
      if next_page_token is None:
        morepages = False
      else:
        request = youtube.playlists().list(part="snippet,contentDetails,status", channelId=get_channel_id, maxResults=50, pageToken=next_page_token)
        response = request.execute()
        
        playlists = {}
        ls = 1
        
        for i in range(0, len(response['items'])):
          data = {'playlist_id': response['items'][i]['id'], 
                  'playlist_name': response['items'][i]['snippet']['title'], 
                  'channel_id': get_channel_id, 
                  'upload_id': get_upload_id}
          title = 'playlist_no_' + str(ls)
          playlists[title] = data
          ls += 1
        next_page_token = response.get('nextPageToken')'''
  except HttpError as e:
    error_message = e.content.decode("utf-8")
    print("An error occurred:", error_message)

  return playlists

# Function to get Video Ids of Playlist
def get_video_ids(youtube, get_uploadid):
  try:
    request = youtube.playlistItems().list(part='contentDetails', playlistId=get_uploadid, maxResults=50)
    response = request.execute()

    videolist = []

    for i in range(0, len(response['items'])):
      data = response['items'][i]['contentDetails']['videoId']
      videolist.append(data)
    '''next_page_token = response.get('nextPageToken')

    morepages = True

    while morepages:
      if next_page_token is None:
        morepages = False
      else:
        request = youtube.playlistItems().list(part='contentDetails', playlistId=get_uploadid, maxResults=50, pageToken=next_page_token)
        response = request.execute()
        
        for i in range(0, len(response['items'])):
          data = response['items'][i]['contentDetails']['videoId']
          videolist.append(data)
        next_page_token = response.get('nextPageToken')'''
  except HttpError as e:
    error_message = e.content.decode("utf-8")
    print("An error occurred:", error_message)

  return videolist

# Function to get Video Details
def get_video_details(youtube, get_videoid, get_uploadid):
  try:
    request = youtube.videos().list(part='contentDetails, snippet, statistics', id=get_videoid)
    response = request.execute()

    cap = {'true': 'Available', 'false': 'Not Available'}

    def time_duration(t):
      a = pd.Timedelta(t)
      b = str(a).split()[-1]
      return b
    
    data = {'video_id': response['items'][0]['id'],
            'video_name': response['items'][0]['snippet']['title'],
            'video_description': response['items'][0]['snippet']['description'],
            'upload_id': get_uploadid,
            'tags': response['items'][0]['snippet'].get('tags', []),
            'published_date': response['items'][0]['snippet']['publishedAt'][0:10],
            'published_time': response['items'][0]['snippet']['publishedAt'][11:19],
            'view_count': response['items'][0]['statistics']['viewCount'],
            'like_count': response['items'][0]['statistics'].get('likeCount', 0),
            'favourite_count': response['items'][0]['statistics']['favoriteCount'],
            'comment_count': response['items'][0]['statistics'].get('commentCount', 0),
            'duration': time_duration(response['items'][0]['contentDetails']['duration']),
            'thumbnail': response['items'][0]['snippet']['thumbnails']['default']['url'],
            'caption_status': cap[response['items'][0]['contentDetails']['caption']]}
    
    if data['tags'] == []:
      del data['tags']

  except HttpError as e:
    error_message = e.content.decode("utf-8")
    print("An error occurred:", error_message)

  return data

# Function to get Comments
def get_comments_details(youtube, get_videoid):
  request = youtube.commentThreads().list(part='id, snippet', videoId=get_videoid, maxResults=50)
  response = request.execute()

  commentslist = {}
  c = 1

  for i in range(0, len(response['items'])):
    data = {'comment_id': response['items'][i]['id'],
            'comment_text': response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
            'comment_author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            'comment_published_date': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][0:10],
            'comment_published_time': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][11:19],
            'video_id': get_videoid}
    c1 = 'comment_no_' + str(c)
    commentslist[c1] = data
    c += 1
  return commentslist

# Function to merge channel details, playlist details and videos with comments
def get_youdata(youtube, get_channeldetails, get_playlistdetails, get_videoiddetails, get_uploadid):
  comments = {}
  videocomments = {}
  ls = 1

  for i in get_videoiddetails:
    videodatadetails = get_video_details(youtube, i, get_uploadid)
    comments.update(videodatadetails)

    if int(videodatadetails['comment_count']) > 0:
        comments_data = get_comments_details(youtube, i)
        comments['comments'] = comments_data
    title = 'video_id_' + str(ls)
    videocomments[title] = comments
    ls += 1
    comments = {}

  data = {'channel_name': get_channeldetails, 'playlists': get_playlistdetails}
  data.update(videocomments)

  return get_channeldetails, videocomments, data
