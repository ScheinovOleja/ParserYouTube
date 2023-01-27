import argparse
import asyncio
import multiprocessing
import threading

import numpy as np
import pandas as pd
from pytube import Channel


def download_video(resolution: int, video, save_path, filename):
    video.streams.get_by_itag(resolution).download(save_path, filename)


async def get_videos(resolution: int, channel: Channel, category: str):
    data = {'title': [], 'description': [], 'url': [], 'path': [], "category": category}
    for video in channel.videos:
        try:
            path = f'./videos/{channel.channel_name}/'
            file = f"{video.video_id}.mp4"
            data['title'].append(video.title)
            data['description'].append(video.description)
            data['url'].append(video.watch_url)
            data['path'].append(path + file)
            task = threading.Thread(target=download_video, args=(resolution, video, path, file,))
            task.start()
            task.join()
        except Exception as e:
            print(f'Ошибка - {e}')
            continue
    new_data = pd.DataFrame.from_dict(data)
    new_data.to_csv(f'result-{channel.channel_name}.csv', sep=';')


def start(data, resolution) -> None:
    tasks = []
    for index, url in enumerate(data['url']):
        try:
            channel = Channel(url)
            task = multiprocessing.Process(target=asyncio.run,
                                           args=(get_videos(resolution, channel, data['category'][index]),))
            tasks.append(task)
        except BaseException as e:
            print(f'Ошибка - {e}')
            continue
    for task in tasks:
        task.start()
    for task in tasks:
        task.join()


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
        resolution = 18
    elif args.resolution == '720p':
        resolution = 22
    elif args.resolution == '1080p':
        resolution = 299
    else:
        resolution = 18
    channels = pd.read_csv('channels.csv', delimiter=';')
    chunks = np.array_split(channels, 1)
    for data in chunks:
        start(data.to_dict('list'), resolution)
