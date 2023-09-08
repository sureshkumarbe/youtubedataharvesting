
from googleapiclient.discovery import build
from datetime import datetime
from PIL import Image
import apiservice as a_service
import dbservice as db_service
import streamlit as st
import pandas as pd
import time

iconimage = Image.open('icon.png')

# Youtube Data API Key from google develper console
# sureshkumarbecbe
api_key='AIzaSyD5hYhDH2NhrHVHWbMXGlrFS1J5fkkcKa8'

# airei.mobileapp
# api_key='AIzaSyB0HorLIu1zWeZoIygutgrsJCv1Xfa498A'

# sureshkumarmecbe
# api_key='AIzaSyB43qNGvJhoq8-C2ljs2A1PPOQiRoKdBL8'

# Create Youtube object
youtube = build('youtube','v3',developerKey=api_key)

# Sample Channel Id
# channel_id='UCduIoIMfD8tT3KoU0-zBRgQ'


st.set_page_config(page_title='YouTube Data Harvesting and Warehousing', page_icon=iconimage, layout="wide")
st.markdown("""
            <style>
                .css-164nlkn.ea3mdgi1{
                    visibility: hidden;
                }
            
                .css-1v0mbdj.e115fcil1 img{
                    border-radius: 50%
                }
            </style>
""", unsafe_allow_html=True)
pd.set_option('display.max_columns', None)

# ---------- Number Formation - Start ----------
def fn_format_number(n):
    if n < 1000:
        return str(n)
    elif n < 1000000:
        formatted = f"{n / 1000:.2f}"
        if formatted.endswith('.00'):
            formatted = formatted[:-3]
        elif formatted.endswith('0'):
            formatted = formatted[:-1]
        return formatted + "k"
    else:
        formatted = f"{n / 1000000:.2f}"
        if formatted.endswith('.00'):
            formatted = formatted[:-3]
        elif formatted.endswith('0'):
            formatted = formatted[:-1]
        return formatted + "M"
# ---------- Number Formation - End ----------

# ---------- Data Extraction - Start ----------
def fn_data_extraction (channel_id):
    # Channel Details of YouTube
    # ---Start---

    # To get the Channel Details using function from apiservice
    channeldetails = a_service.get_channel_details(youtube, channel_id)
    # ---End---

    # Playlist Details of YouTube
    # ---Start---

    # Uploadid from channeldetails to get the playlist details
    upload_id = channeldetails['upload_id']

    # To get the Playlist Details using function from apiservice
    playlistdetails = a_service.get_playlists_details(youtube, channel_id, upload_id)
    # ---End---

    # Video Id Details of YouTube
    # ---Start---

    # To get the Video Id Details using function from apiservice
    videoiddetails = a_service.get_video_ids(youtube, upload_id)
    # ---End---

    # To get the consolidated data of channel, playlist, videos with comments
    # ---Start---

    channeldata, videodata, youtubedata = a_service.get_youdata(youtube, channeldetails, playlistdetails, videoiddetails, upload_id)
    # ---End---


    return channeldata, videodata, youtubedata
# ---------- Data Extraction - End ----------

# Database Name
dbname = 'youtube_data'



st.markdown("<h1 style='text-align: center; color: red;'>YouTube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)
st.markdown("""---""")

col1, col2, col3 = st.columns([2,8,2])

with col2:
    channel_id = st.text_input("Channel ID: ", placeholder='Please enter YouTube Channel ID', label_visibility='visible')


# Session for buttons
if 'stage' not in st.session_state:
    st.session_state.stage = 0

def set_stage(stage):
    st.session_state.stage = stage

columns = st.columns([5, 2, 5])
btn_search = columns[1].button('Search', on_click=set_stage, args=(1,))

if st.session_state.stage > 0:
    if channel_id != '':
        data_youtube = {}
        channel_data, video_data, data_extraction = fn_data_extraction(channel_id)
        data_youtube.update(data_extraction)

        channel_name = data_youtube['channel_name']['channel_name']
        channel_description = data_youtube['channel_name']['channel_description']
        channel_subscriber_count = fn_format_number(int(data_youtube['channel_name']['subscription_count']))
        channel_view_count = fn_format_number(int(data_youtube['channel_name']['channel_views']))
        channel_video_count = fn_format_number(int(data_youtube['channel_name']['channel_videos']))
        channel_image = data_youtube['channel_name']['thumbnail_image']
        channel_createddate = datetime.strptime(data_youtube['channel_name']['created_date'], '%Y-%m-%d').strftime('%d-%m-%Y')
        channel_link = data_youtube['channel_name']['channel_link']

        st.markdown("""---""")
        st.markdown(f"""<h4 style='text-align: center; color: #0ba844;'><span style='color:red; font-weight:bold'>YouTube </span>Channel Name : {channel_name}</h4>""", unsafe_allow_html=True)

        scol1, scol2, scol3, scol4, scol5 = st.columns([0.2, 0.1, 0.2, 0.1, 0.4])

        with scol1:
            col1, col2, col3 = st.columns([1.5,5,1.5])
            with col1:
                st.write("")

            with col2:
                st.image(channel_image, width=150)

            with col3:
                st.write("")
                
        with scol3:
            st.write("<h4 style='text-align: center; color: red;'>Channel Created Date</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'><h4>{channel_createddate}</h4></div>""", unsafe_allow_html=True)

        with scol5:
            st.write("<h4 style='text-align: center; color: red;'>Channel Link</h4>", unsafe_allow_html=True)
            link_html = f"<div style='text-align: center;'><a href={channel_link} target='_blank'>{channel_link}</a></div>"
            st.markdown(link_html, unsafe_allow_html=True)

        st.markdown('<br>', unsafe_allow_html=True)

        titl1, titl2, titl3, titl4, titl5 = st.columns([0.2, 0.1, 0.2, 0.1, 0.4])

        with titl1:
            st.markdown("<h4 style='text-align: center; color: #0ba844;'>Subcribers</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'>
                            <h5>{channel_subscriber_count}</h5>
                            </div>""", unsafe_allow_html=True)

        with titl3:
            st.markdown("<h4 style='text-align: center; color: #0ba844;'>Total Views</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'>
                            <h5>{channel_view_count}</h5>
                            </div>""", unsafe_allow_html=True)

        with titl5:
            st.markdown("<h4 style='text-align: center; color: #0ba844;'>Total Videos</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'>
                            <h5>{channel_video_count}</h5>
                            </div>""", unsafe_allow_html=True)
        st.markdown("""---""")

        db_service.drop_temp_collection()

        db_service.store_collection(channel_name=channel_name, database='temp_youtube_data', data_youtube=data_youtube)

        st.markdown(f"""<h4 style='text-align: center; color: #0068c9;'>Migrate Data to MongoDB</h4>""", unsafe_allow_html=True)

        tab1,tab2,tab3 = st.tabs(['Channel Data','Video Data','Consolidated Data'])

        with tab1:
            with st.expander("Channel Data"):
                channel_tab = st.empty()
                if channel_data:
                    channel_tab.json(channel_data,expanded=False)
        
        with tab2:
            with st.expander("Videos Data"):
                video_tab = st.empty()
                if video_data:
                    video_tab.json(video_data,expanded=False)

        with tab3:
            with st.expander("Consolidated Data"):
                consolidated_tab = st.empty()
                if data_youtube:
                    consolidated_tab.json(data_youtube,expanded=False)
        
        if btn_search:
            st.balloons()

            successalert = st.success('Retrived data from YouTube successfully')
            time.sleep(3) # Wait for 3 seconds
            successalert.empty()

        mongodbuploadcolumns = st.columns([5, 2, 5])
        btn_mongodbupload = mongodbuploadcolumns[1].button('Upload to MongoDB', on_click=set_stage, args=(2,))

        if st.session_state.stage > 1:
            if btn_mongodbupload:
                db_service.store_mongodb(channel_name, dbname)
                st.session_state.stage = 3

        if st.session_state.stage > 2:
            st.markdown("""---""")

            bcol1, bcol2, bcol3 = st.columns([2,8,2])
            
            with bcol2:
                st.markdown(f"""<h4 style='text-align: center; color: #0068c9;'>Migrate Data to SQL</h4>""", unsafe_allow_html=True)
                        
                db_service.store_sql(dbname)

                st.session_state.stage = 4

        if st.session_state.stage > 3:
            st.markdown("""---""")

            qcol1, qcol2, qcol3 = st.columns([2,8,2])

            with qcol2:
                st.markdown(f"""<h4 style='text-align: center; color: #0068c9;'>Data Analysis</h4>""", unsafe_allow_html=True)
                            
                db_service.sql_qureries(dbname)
    else:
        c_warning = st.warning("Please enter Channel ID")
        time.sleep(3) # Wait for 3 seconds
        c_warning.empty()
