from extracting.extract import extract_data
from loading.load import *
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


if __name__=='__main__':
    
    #extracting data from api youtube api
    data = extract_data()
    
    #creating connection
    conn = connect_to_db()
    cursor = conn.cursor()
    
    #creating videos table
    create_table(cursor)
    
    #updating existing rows , if video doesnt exist returns new videos dataframe
    new_videos = update_db(cursor, data)
    logging.info("Updated existing rows")
    
    #appending table with new videos
    append_from_df_to_db(cursor, new_videos)
    if not new_videos.empty:
        logging.info("Inserted new rows")
    
    #Committing and closing connection
    conn.commit()
    conn.close()