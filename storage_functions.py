#%%
import os
import logging
import pandas as pd

from pandas import DataFrame
from datetime import date, datetime
from botocore.exceptions import ClientError
from pathlib import Path
base_path = Path(__file__).parents[0]

import credentials

class Storage_Functions():
    def __init__(self) -> None:
        self.client = credentials.get_aws_client(service='s3') 

    def list_buckets(self) -> list:
        try:
            all_buckets =  self.client.list_buckets()
            
            list_buckets = [bucket['Name'] for bucket in all_buckets['Buckets']]
        except ClientError as e:
            logging.error(f'e')

        return list_buckets

    def list_partitions(self, bucket_name:str) -> set:
        try:
            bucket_ref = self.client.list_objects(Bucket=bucket_name)
        
            partitions_objs = [obj['Key'] for obj in bucket_ref['Contents']]
        
            partitions_objs = set(map(lambda name:name.split('/')[0], partitions_objs))

        except ClientError as e:
            logging.error(f'e')

        return partitions_objs

    def list_partition_objects(self, bucket_name:str, partition_name:str) -> set:
        try:
            bucket_ref = self.client.list_objects(Bucket=bucket_name)
            all_objs = [obj['Key'] for obj in bucket_ref['Contents']]

            filter_objs = filter(lambda name:partition_name in name, all_objs) 
        
            filter_objs = set(map(lambda name:name.split('/')[1], filter_objs))

        except ClientError as e:
            logging.error(f'e')

        return filter_objs

    def insert_table_from_df(self, 
                    bucket_name:str, 
                    zone_name:str, 
                    dataset_name:str,
                    dt:date,
                    df:DataFrame,
                    format:str='csv') -> None:
        
        temporary_file = f'temp_{dt}.{format}'

        if format == 'csv':
            df.to_csv(f'{temporary_file}', encoding='utf-8', index=False)
        elif format == 'json':
            df.to_json(f'{temporary_file}', orient='records')
        elif format == 'parquet':
            df.to_parquet(f'{temporary_file}', orient='records', engine='pyarrow')
            
        target_path = f'{zone_name}/{dataset_name}/{dt}.{format}'
        
        with open(f'{temporary_file}', 'rb') as f:
            self.client.upload_fileobj(f, bucket_name, target_path)
            
        os.remove(f'{temporary_file}')

        logging.info(f'A tabela {dataset_name}.{dt} foi inserido com sucesso')
        