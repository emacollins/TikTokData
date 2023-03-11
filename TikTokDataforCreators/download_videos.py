import asyncio
import io
import glob
import os
import urllib.request
import datetime
from os import path
import pandas as pd

import aiohttp
from tiktokapipy.async_api import AsyncTikTokAPI
from tiktokapipy.models.video import Video
from tiktokapipy.models.video import video_link


async def save_slideshow(video: Video):
    # this filter makes sure the images are padded to all the same size
    vf = "\"scale=iw*min(1080/iw\,1920/ih):ih*min(1080/iw\,1920/ih)," \
         "pad=1080:1920:(1080-iw)/2:(1920-ih)/2," \
         "format=yuv420p\""

    for i, image_data in enumerate(video.image_post.images):
        url = image_data.image_url.url_list[-1]
        # this step could probably be done with asyncio, but I didn't want to figure out how
        urllib.request.urlretrieve(url, path.join(directory, f"temp_{video.id}_{i:02}.jpg"))

    urllib.request.urlretrieve(video.music.play_url, path.join(directory, f"temp_{video.id}.mp3"))

    # use ffmpeg to join the images and audio
    command = [
        "ffmpeg",
        "-r 2/5",
        f"-i {directory}/temp_{video.id}_%02d.jpg",
        f"-i {directory}/temp_{video.id}.mp3",
        "-r 30",
        f"-vf {vf}",
        "-acodec copy",
        f"-t {len(video.image_post.images) * 2.5}",
        f"{directory}/temp_{video.id}.mp4",
        "-y"
    ]
    ffmpeg_proc = await asyncio.create_subprocess_shell(
        " ".join(command),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await ffmpeg_proc.communicate()
    generated_files = glob.glob(path.join(directory, f"temp_{video.id}*"))

    if not path.exists(path.join(directory, f"temp_{video.id}.mp4")):
        # optional ffmpeg logging step
        # logging.error(stderr.decode("utf-8"))
        for file in generated_files:
            os.remove(file)
        raise Exception("Something went wrong with piecing the slideshow together")

    with open(path.join(directory, f"temp_{video.id}.mp4"), "rb") as f:
        ret = io.BytesIO(f.read())

    for file in generated_files:
        os.remove(file)

    return ret

async def save_video(video: Video):
    async with aiohttp.ClientSession() as session:
        async with session.get(video.video.download_addr) as resp:
            return io.BytesIO(await resp.read())

def check_for_video_file(video_id, user):
    """If video file exists, returns False and video will not be saved"""
    file = f"{user}_{str(video_id)}.mp4"
    if file not in os.listdir('/Users/ericcollins/TikTokData/TikTokDataforCreators/videos'):
        return True
    else:
        return False
async def download_video(video_id: int, user: str):
    # mobile emulation is necessary to retrieve slideshows
    # if you don't want this, you can set emulate_mobile=False and skip if the video has an image_post property
    link = video_link(video_id)
    async with AsyncTikTokAPI(emulate_mobile=True) as api:
        video: Video = await api.video(link)
        if video.image_post:
            downloaded = await save_slideshow(video)
        else:
            downloaded = await save_video(video)

    if check_for_video_file(video_id, user):
        filepath = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/videos/{user}_{str(video_id)}.mp4'
        with open(filepath, "wb") as f:
            f.write(downloaded.getvalue())


def run():
    date_string = datetime.datetime.now().date().strftime('%m-%d-%Y')
    extract_file = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/extract/extract_{date_string}.csv'
    extract_data = pd.read_csv(extract_file)
    extract_data.apply(lambda x: asyncio.run(download_video(video_id=x['video_id'], 
                                                            user=x['user_unique_id'])), axis=1)


if __name__ == '__main__':
    run()