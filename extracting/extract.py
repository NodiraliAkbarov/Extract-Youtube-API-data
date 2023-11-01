import requests
from dotenv import load_dotenv
import pandas as pd
import logging
import os


load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_video_details(video_id:str):

    #collecting view, like, dislike, comment counts by video_id 
    url_video_stats = "https://www.googleapis.com/youtube/v3/videos?id="+video_id+"&part=statistics&key="+os.getenv("api_key")
    response_video_stats = requests.get(url_video_stats).json()

    view_count = response_video_stats['items'][0]['statistics']['viewCount']
    like_count = response_video_stats['items'][0]['statistics']['likeCount']
    comment_count = response_video_stats['items'][0]['statistics']['commentCount']

    return view_count, like_count, comment_count


def extract_data():

    try : 

        df = pd.DataFrame(columns=["video_id","video_title","upload_time","view_count","like_count","comment_count"])
        
        pageToken=""
        
        url = "https://www.googleapis.com/youtube/v3/search?key="+os.getenv("api_key")+"&channelId="+os.getenv("channel_id")+\
        "&part=snippet,id&order=date&maxResults=5000&"+pageToken
        
        response = requests.get(url).json()
        logging.info("Exctracted data from API")

    except Exception as e:

        logging.error(f"{e}")

    
    try : 
        for video in response['items']:
            if video['id']['kind'] == "youtube#video":
                video_id = video['id']['videoId']
                video_title = video['snippet']['title']
                upload_date = video['snippet']['publishedAt']
                upload_date = str(upload_date).split("T")[0]
            view_count, like_count, comment_count = get_video_details(video_id)
            df = df._append({"video_id":video_id,"video_title":video_title,"upload_date":upload_date, "view_count":view_count, "like_count":like_count,
                    "comment_count":comment_count}, ignore_index=True)
        logging.info(f"Transformed {df.shape[0]} rows succesfully")
        
         
    except  Exception as e:
        logging.error(f"{e}")

    return df
