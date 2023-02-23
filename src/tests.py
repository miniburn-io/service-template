from time import sleep
import os
from threading import Thread
from boto3 import client
from pydantic import BaseModel
from src.streaming.kinesis import KinesisStream
from dotenv import load_dotenv

load_dotenv()

stream_name = os.getenv('STREAM_NAME')


class Message(BaseModel):
    body: str

__client = client('kinesis', endpoint_url='https://yds.serverless.yandexcloud.net')
streamer = KinesisStream(__client, 'stream-mb-projects', Message)


def run():
    while True:
        streamer.put(Message(body="Hello"))
        sleep(1)

thread = Thread(target=run)
thread.start()

for record in streamer.read():
    print(record)

thread.join()
