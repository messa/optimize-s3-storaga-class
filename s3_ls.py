#!/usr/bin/env python3

from argparse import ArgumentParser
import boto3
from logging import getLogger
from pathlib import Path
import re
import sys


logger = getLogger(Path(__file__).with_suffix('').name)

log_format = '%(asctime)s %(name)s %(levelname)5s: %(message)s'


def main():
    p = ArgumentParser()
    p.add_argument('s3_url')
    args = p.parse_args()
    setup_logging()
    m = re.match(r'^s3://([^/]+)(?:/(.*))?', args.s3_url)
    if not m:
        sys.exit('Invalid S3 URL format: ' + args.s3_url)
    bucket_name, key_prefix = m.groups()
    key_prefix = key_prefix or ''
    logger.debug('bucket_name: %r', bucket_name)
    logger.debug('key_prefix: %r', key_prefix)
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=key_prefix)
    total_count = 0
    for page in page_iterator:
        if not page.get('Contents'):
            logger.warning('No key Contents in page: %r', page)
            continue
        for obj in page['Contents']:
            print(bucket_name, obj['Key'], obj['Size'], obj['StorageClass'])
            total_count += 1
        logger.info('Listed %d objects', total_count)


def setup_logging():
    from logging import basicConfig, getLogger, DEBUG, INFO
    basicConfig(format=log_format, level=DEBUG)
    getLogger('botocore.hooks').setLevel(INFO)
    getLogger('botocore.loaders').setLevel(INFO)
    getLogger('botocore.auth').setLevel(INFO)
    getLogger('botocore.endpoint').setLevel(INFO)
    getLogger('botocore.parsers').setLevel(INFO)
    getLogger('botocore.utils').setLevel(INFO)
    getLogger('botocore.retryhandler').setLevel(INFO)


if __name__ == '__main__':
    main()
