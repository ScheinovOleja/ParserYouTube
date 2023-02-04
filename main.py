import argparse
import asyncio
import multiprocessing
import os.path

import aiocsv
import aiofiles
import numpy as np
import pandas as pd
from pytube import Channel


async def download_video(itag: int, video, save_path, filename):
    try:
        video.streams.get_by_itag(itag).download(save_path, filename)
    except BaseException:
        return


async def init_csv(channel):
    fieldnames = ["channel_id", "channel_name", "title", "description", "video", "category"]
    async with aiofiles.open(f'csv/result-{channel.channel_name}.csv', encoding='utf-8', mode='w') as file:
        writer = aiocsv.AsyncDictWriter(file, fieldnames, delimiter=';')
        await writer.writeheader()


async def get_videos(itag: int, channel: Channel, category: str, channel_id):
    fieldnames = ["channel_id", "channel_name", "title", "description", "video", "category", ]
    data = {"channel_id": '', "channel_name": '', "title": '', "description": '', "video": '', "category": category}
    for video in channel.videos:
        path = os.path.abspath(f'./videos/{channel.channel_name}/')
        file = f"{video.video_id}.mp4"
        data["channel_id"] = channel_id
        data['channel_name'] = channel.channel_name
        data['title'] = video.title
        data['description'] = video.description
        data['video'] = path + file
        await download_video(itag, video, path, file)
        async with aiofiles.open(f'csv/result-{channel.channel_name}.csv', encoding='utf-8', mode='a+') as file:
            writer = aiocsv.AsyncDictWriter(file, fieldnames, delimiter=';')
            await writer.writerow(data)


def start(data, itag) -> None:
    tasks = []
    for index, url in enumerate(data['url']):
        try:
            channel = Channel(url)
            task = multiprocessing.Process(target=asyncio.run,
                                           args=(get_videos(itag, channel, data['category'][index],
                                                            data['channel_id'][index]),))
            tasks.append(task)
            asyncio.run(init_csv(channel))
        except BaseException as e:
            print(f'Ошибка - {e}')
            continue
    for task in tasks:
        task.start()
    for task in tasks:
        task.join()
        task.close()
        exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Парсер сайтов брендовой одежды',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-r', '--resolution', type=str,
                        help="""Выбор качества видео.
[360p, 720p, 1080p]""")
    args = parser.parse_args()
    if not args.resolution:
        args.resolution = '360p'
    if args.resolution == '360p':
        itag = 18
    elif args.resolution == '720p':
        itag = 22
    elif args.resolution == '1080p':
        itag = 299
    else:
        itag = 18
    channels = pd.read_csv('channels.csv', delimiter=';')
    chunks = np.array_split(channels, 1)
    for data in chunks:
        start(data.to_dict('list'), itag)
