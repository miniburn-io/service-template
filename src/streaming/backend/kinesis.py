import os
from boto3 import client
from loguru import logger
from typing import Generator, Type
from pydantic import BaseModel
from src.streaming.backend.abstract import StreamBackend
from botocore.client import ClientError
from itertools import cycle


class KinesisStream(StreamBackend):
    def __init__( self, stream_name: str, record_model: Type[BaseModel]) -> None:
        super().__init__(stream_name, record_model)
        self.__client = client('kinesis', endpoint_url='https://yds.serverless.yandexcloud.net')

    def read(self) -> Generator[BaseModel, None, None]:
        try:
            describe_stream_result = self.__client.describe_stream(StreamName=self._stream_name)
            shard_iterators = {}

            shards = [shard["ShardId"] for shard in describe_stream_result['StreamDescription']['Shards']]

            for shard_id in cycle(shards):
                if shard_id not in shard_iterators:
                    shard_iterators[shard_id] = self.__client.get_shard_iterator(
                        StreamName=self._stream_name,
                        ShardId=shard_id,
                        ShardIteratorType='LATEST'
                    )['ShardIterator']

                record_response = self.__client.get_records(ShardIterator=shard_iterators[shard_id])
                if "Records" in record_response:
                    for record in [record for record in record_response["Records"]]:
                        try:
                            yield self._record_model.parse_raw(record["Data"])
                        except ValueError as err:
                            logger.error(
                                "Can't parse record: {} - {}\n{}",
                                self._stream_name,
                                err,
                                record["Data"]
                            )

                if "NextShardIterator" in record_response:
                    shard_iterators[shard_id] = record_response["NextShardIterator"]
        except ClientError:
            logger.exception("Couldn't get records from the stream {}", self._stream_name)
            raise


    def put(self, message: BaseModel) -> None:
        try:
            self.__client.put_record(
                StreamName=self._stream_name,
                Data=message.json(),
                PartitionKey=self._stream_name,)
            logger.info("Put record in stream {}", self._stream_name)
        except ClientError:
            logger.exception("Couldn't put record in stream {}", self._stream_name)
            raise

    @staticmethod
    def get_full_stream_name(stream_name: str) -> str:
        REGION = os.getenv("REGION")
        CATALOG_ID = os.getenv("CATALOG_ID")
        DATABASE_ID = os.getenv("DATABASE_ID")

        return f"/{REGION}/{CATALOG_ID}/{DATABASE_ID}/{stream_name}"


