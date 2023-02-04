import asyncio
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine, text, Connection
from sqlalchemy.orm import sessionmaker, Session


async def handler(item, session: Session, conn: Connection):
    with session:
        query_category = text(f"""
        select * from categories where name='{item['category']}'
        """)
        category = conn.execute(query_category).fetchone()
        query_create_video = text(f"""
        insert into video (channel_id, title, description, video, created_at)
        values ({item['channel_id']}, '{item['title']}', '{item['description']}', '{item['video']}', {int(time())})
        """)
        video = conn.execute(query_create_video)
        query_create_category_to_video = text(f"""
        insert into categories_to_video (category_id, video_id) 
        values ({category[0]}, {video.lastrowid})
        """)
        conn.execute(query_create_category_to_video)
        conn.commit()
    print(video)


if __name__ == '__main__':
    engine = create_engine("mysql+pymysql://ru24_user:YP%EkCh2@195.80.51.150/ru24_ru24?charset=utf8mb4")
    conn = engine.connect()
    Session = sessionmaker(engine)
    for csv in os.listdir('csv/'):
        data_csv = pd.read_csv("csv/" + csv, delimiter=";")
        for item in data_csv.iterrows():
            asyncio.run(handler(item[1].to_dict(), Session(), conn))
