#!/usr/bin/env python3

import argparse
import asyncio
import aiohttp
import boto3
import json
import logging
import sys
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin
from botocore.config import Config

# Configure logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

@dataclass
class OpenNeuroFile:
    filename: str
    size: int
    urls: List[str]
    directory: bool

class OpenNeuroDownloader:
    OPENNEURO_API = "https://openneuro.org/crn/graphql"
    CHUNK_SIZE = 1024 * 1024  # 1MB chunks for streaming

    def __init__(self, s3_bucket: str, aws_profile: Optional[str] = None):
        self.s3_bucket = s3_bucket
        
        # Use custom retry config for S3
        config = Config(
            retries=dict(
                max_attempts=10,
                mode='adaptive'
            ),
            max_pool_connections=50
        )
        
        session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
        self.s3_client = session.client('s3', config=config)
        
        # Validate bucket exists and we have access
        try:
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
        except Exception as e:
            logger.error(f"Failed to access S3 bucket {self.s3_bucket}: {e}")
            raise

    async def _graphql_query(self, session: aiohttp.ClientSession, query: str, variables: Dict[str, Any] = None) -> Dict:
        """Execute GraphQL query with error handling and retries."""
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                async with session.post(
                    self.OPENNEURO_API,
                    json={'query': query, 'variables': variables}
                ) as response:
                    if response.status != 200:
                        text = await response.text()
                        logger.error(f"GraphQL request failed with status {response.status}: {text}")
                        raise Exception(f"GraphQL request failed: {response.status}")
                    
                    result = await response.json()
                    if 'errors' in result:
                        logger.error(f"GraphQL errors: {result['errors']}")
                        raise Exception(f"GraphQL query failed: {result['errors']}")
                    
                    return result['data']
            
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2

    async def _get_dataset_files(self, session: aiohttp.ClientSession, dataset_id: str) -> List[OpenNeuroFile]:
        """Recursively fetch all files in the dataset."""
        query = """
        query Dataset($datasetId: ID!) {
            dataset(id: $datasetId) {
                draft {
                    files {
                        filename
                        size
                        urls
                        directory
                    }
                }
            }
        }
        """
        
        result = await self._graphql_query(session, query, {'datasetId': dataset_id})
        files = result['dataset']['draft']['files']
        
        return [OpenNeuroFile(**f) for f in files]

    async def _stream_to_s3(self, session: aiohttp.ClientSession, file: OpenNeuroFile, prefix: str = '') -> None:
        """Stream file directly from OpenNEURO to S3 using multipart upload."""
        if file.directory:
            logger.debug(f"Skipping directory: {file.filename}")
            return

        s3_key = f"{prefix}/{file.filename}" if prefix else file.filename
        logger.debug(f"Streaming {file.filename} ({file.size} bytes) to s3://{self.s3_bucket}/{s3_key}")

        # Choose the best URL (prefer S3 direct if available)
        download_url = next((url for url in file.urls if 's3://' in url), file.urls[0])

        try:
            # Initialize multipart upload
            mpu = self.s3_client.create_multipart_upload(
                Bucket=self.s3_bucket,
                Key=s3_key,
                ServerSideEncryption='AES256'
            )

            parts = []
            part_number = 1

            async with session.get(download_url, timeout=3600) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download file: HTTP {response.status}")

                while True:
                    chunk = await response.content.read(self.CHUNK_SIZE)
                    if not chunk:
                        break

                    # Upload part
                    part = self.s3_client.upload_part(
                        Bucket=self.s3_bucket,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=mpu['UploadId'],
                        Body=chunk
                    )

                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    
                    logger.debug(f"Uploaded part {part_number} of {s3_key}")
                    part_number += 1

            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.s3_bucket,
                Key=s3_key,
                UploadId=mpu['UploadId'],
                MultipartUpload={'Parts': parts}
            )

            logger.info(f"Successfully uploaded {s3_key}")

        except Exception as e:
            logger.error(f"Failed to upload {s3_key}: {e}")
            # Abort multipart upload if it was started
            if 'mpu' in locals():
                try:
                    self.s3_client.abort_multipart_upload(
                        Bucket=self.s3_bucket,
                        Key=s3_key,
                        UploadId=mpu['UploadId']
                    )
                except Exception as abort_error:
                    logger.error(f"Failed to abort multipart upload: {abort_error}")
            raise

    async def download_dataset(self, dataset_id: str) -> None:
        """Download entire dataset to S3 bucket."""
        async with aiohttp.ClientSession() as session:
            try:
                files = await self._get_dataset_files(session, dataset_id)
                logger.info(f"Found {len(files)} files in dataset {dataset_id}")

                # Create semaphore to limit concurrent uploads
                semaphore = asyncio.Semaphore(10)
                
                async def upload_with_semaphore(file: OpenNeuroFile):
                    async with semaphore:
                        await self._stream_to_s3(session, file)

                # Start all uploads concurrently
                tasks = [upload_with_semaphore(file) for file in files if not file.directory]
                await asyncio.gather(*tasks)

            except Exception as e:
                logger.error(f"Failed to process dataset {dataset_id}: {e}")
                raise

def main():
    parser = argparse.ArgumentParser(description='Download OpenNEURO datasets to S3')
    parser.add_argument('dataset_ids', nargs='+', help='OpenNEURO dataset IDs to download')
    parser.add_argument('--s3-bucket', required=True, help='Target S3 bucket')
    parser.add_argument('--aws-profile', help='AWS profile to use')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    # Set log level
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger.setLevel(log_level)

    downloader = OpenNeuroDownloader(args.s3_bucket, args.aws_profile)

    for dataset_id in args.dataset_ids:
        try:
            asyncio.run(downloader.download_dataset(dataset_id))
            logger.info(f"Successfully downloaded dataset {dataset_id}")
        except Exception as e:
            logger.error(f"Failed to download dataset {dataset_id}: {e}")
            sys.exit(1)

if __name__ == '__main__':
    main()
