import asyncio
import multiprocessing
import threading
import numpy as np
import pandas as pd
from pytube import Channel


def download_video(resolution: int, video, save_path, filename):
    video.streams.get_by_itag(resolution).download(save_path, filename)


async def get_videos(resolution: int, channel: Channel):
    data = {'title': [], 'description': [], 'url': [], 'path': []}
    for video in channel.videos:
        path = f'./videos/{channel.channel_name}/'
        file = f"{video.video_id}.mp4"
        task = threading.Thread(target=download_video, args=(resolution, video, path, file,))
        data['title'].append(video.title)
        data['description'].append(video.description)
        data['url'].append(video.watch_url)
        data['path'].append(path + file)
        task.start()
        task.join()
    new_data = pd.DataFrame.from_dict(data)
    new_data.to_csv(f'result-{channel.channel_name}.csv')


def start(data) -> None:
    tasks = []
    for index, url in enumerate(data['url']):
        channel = Channel(url)
        task = multiprocessing.Process(target=asyncio.run, args=(get_videos(22, channel),))
        tasks.append(task)
    for task in tasks:
        task.start()
    for task in tasks:
        task.join()


if __name__ == '__main__':
    channels = pd.read_csv('channels.csv', delimiter=';')
    chunks = np.array_split(channels, 1)
    for data in chunks:
        start(data.to_dict('list'))
