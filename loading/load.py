from dotenv import load_dotenv
import pandas as pd
import logging
import os
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()


def connect_to_db():
    """A function creates connection on database"""
    try :
        conn = psycopg2.connect(host=os.getenv("host"), database=os.getenv("db"), 
                                port=os.getenv("port"), user=os.getenv("user"), password=os.getenv("password"))
        
    except Exception as e:
        logging.error(f"{e}")
        
    else:
        logging.info("Connected to database")

    return conn



def create_table(curr):
    """A function creates videos table"""

    command_create_table = """CREATE TABLE IF NOT EXISTS videos (video_id VARCHAR(100),
                    video_title TEXT NOT NULL,
                    upload_date TEXT NOT NULL,
                    view_count INTEGER NOT NULL,
                    like_count INTEGER NOT NULL,
                    comment_count INTEGER NOT NULL)"""
    
    try : 
        curr.execute(command_create_table)
        logging.info("Table created")
    
    except Exception as e:
        logging.error(f"{e}")

def insert_into_table(curr, video_id, video_title, upload_date, view_count, like_count, comment_count):

    insert_into_table_query = """INSERT INTO videos (video_id, video_title, upload_date, view_count, like_count, comment_count)
                   VALUES(%s,%s,%s,%s,%s,%s)"""
    rows_to_insert = (video_id, video_title, upload_date, view_count, like_count, comment_count)
    try : 
         curr.execute(insert_into_table_query, rows_to_insert)
        
    except Exception as e:
          logging.error(f"{e}")
        

def update_existing_rows(curr, video_id, video_title, view_count, like_count, comment_count):
    update_rows_query = """UPDATE videos SET video_title = %s,
                view_count = %s,
                like_count = %s,      
                comment_count = %s
                WHERE video_id = %s;"""
    rows_to_update = (video_title, view_count, like_count, comment_count, video_id)
    try : 
        curr.execute(update_rows_query, rows_to_update)
        
    except Exception as e:
        logging.error(f"{e}")

def check_if_video_exists(curr, video_id):
    check_query = """SELECT video_id FROM videos WHERE video_id = %s"""
    curr.execute(check_query, (video_id,))
    return curr.fetchone() is not None

def append_from_df_to_db(curr, df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['video_id'], row['video_title'], row['upload_date'], row['view_count']
                          , row['like_count'], row['comment_count'])
def update_db(curr, df):
    tmp_df = pd.DataFrame(columns=['video_id', 'video_title', 'upload_date', 'view_count',
                                   'like_count',  'comment_count'])
    for i, row in df.iterrows():
        if check_if_video_exists(curr, row['video_id']): 
            update_existing_rows(curr,row['video_id'],row['video_title'],row['view_count'],row['like_count'],row['comment_count'])
        else: 
            tmp_df = tmp_df._append(row)

    return tmp_df

