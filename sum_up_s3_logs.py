#!/usr/bin/env python3

from argparse import ArgumentParser
import boto3
from collections import Counter, namedtuple
from datetime import datetime
from functools import partial
import gzip
from logging import getLogger
import multiprocessing
from pathlib import Path
import re
import sys


logger = getLogger(Path(__file__).with_suffix('').name)

log_format = '%(asctime)s [%(process)d %(processName)s] %(name)s %(levelname)5s: %(message)s'

output_lock = multiprocessing.Lock()


def main():
    p = ArgumentParser()
    p.add_argument('s3_log_path', nargs='+')
    args = p.parse_args()
    setup_logging()
    tasks = get_tasks(args.s3_log_path)
    with multiprocessing.Pool() as pool:
        results = []
        for n, task in enumerate(tasks, start=1):
            results.append(pool.apply_async(partial(task,
                task_number=n,
                task_count=len(tasks))))
        for res in results:
            res.get()


def get_tasks(paths):
    tasks = []
    for p in paths:
        assert isinstance(p, str)
        if p == '-':
            tasks.append(partial(process_stream, sys.stdin.buffer, path='-'))
        elif p.startswith('s3://'):
            raise Exception('S3 URLs not supported yet')
        else:
            for file_path in iter_file_paths(Path(p)):
                tasks.append(partial(process_file, file_path))
    return tasks


def iter_file_paths(p):
    if not p.exists():
        raise Exception("Path doesn't exist: {}".format(p))
    if p.is_file():
        yield p
    elif p.is_dir():
        for pp in sorted(p.iterdir()):
            yield from iter_file_paths(pp)


def process_file(file_path, **kwargs):
    try:
        if file_path.name.endswith('.gz'):
            with gzip.open(file_path, mode='rb') as f:
                process_stream(f, path=file_path, **kwargs)
        else:
            with file_path.open(mode='rb') as f:
                process_stream(f, path=file_path, **kwargs)
    except Exception as e:
        logger.exception('Failed to process file %s: %r', file_path, e)


def process_stream(stream, path, task_number, task_count):
    try:
        with output_lock:
            logger.info('Processing file %5d/%d: %s', task_number, task_count, path)
        counter = Counter()
        for n, line in enumerate(stream, start=1):
            assert isinstance(line, bytes)
            if n % 100000 == 0:
                logger.info('Processed %9d lines', n)
            if line.startswith(b'#'):
                continue
            rec = parse_line(line)
            k = (rec.bucket, rec.key, rec.date.strftime('%Y-%m-%d'), rec.operation)
            counter[k] += 1
            if len(counter) >= 1000000:
                print_counts(counter)
                counter = Counter()
        if counter:
            print_counts(counter)
    except Exception as e:
        logger.exception('Failed to process stream %s: %r', path, e)


def print_counts(counter):
    with output_lock:
        for k, n in counter.items():
            bucket, key, day, operation = k
            print('{} {} date={} operation={} count={}'.format(bucket, key, day, operation, n))
            sys.stdout.flush()


def list_file_paths(p):
    if p.is_file():
        yield p
    elif p.is_dir():
        for pp in p.iterdir():
            yield from list_file_paths(pp)


re_line = re.compile(
    rb'^[0-9a-f]+ ([^ ]+) \[([0-9A-Za-z:/ +-]+)\] [^ ]+ [^ ]+ [^ ]+ '
    rb'([^ ]+) ([^ ]+)'
    rb'.*'
)


Record = namedtuple('Record', 'bucket date operation key')


def parse_line(line):
    assert isinstance(line, bytes)
    m = re_line.match(line)
    if not m:
        raise Exception('Invalid line format')
    bucket, dt_str, operation, key = m.groups()
    return Record(
        bucket.decode('ascii'),
        parse_date(dt_str),
        operation.decode('ascii'),
        key.decode('UTF-8'))


re_date = re.compile(
    rb'([0123][0-9])/([A-Z][a-z]{2})/([0-9]{4}):'
    rb'([012][0-9]):([0-5][0-9]):([0-6][0-9]) ([+-])([0-9]{2})([0-9]{2})'
)


month_names = b'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()
month_by_name = {name: n for n, name in enumerate(month_names, start=1)}
assert month_by_name[b'Jan'] == 1
assert month_by_name[b'Dec'] == 12


def parse_date(dt_str):
    assert isinstance(dt_str, bytes)
    m = re_date.match(dt_str)
    if not m:
        raise Exception('Invalid date format: {!r}'.format(dt_str))
    day, month_name, year, hour, minute, second, tz_sign, tz_hours, tz_minutes = m.groups()
    if tz_hours != b'00' or tz_minutes != b'00':
        # never happened, but just to be sure
        raise Exception('Timezone support other than 0000 not yet implemented :/')
    return datetime(
        int(year), month_by_name[month_name], int(day),
        int(hour), int(minute), int(second))


sample_line = (
    'c8f54323b37925ebe7617174b0e940623caad6e9e798e7f0dc55675743eb04c0 '
    'edc-data-staging [03/May/2019:02:40:29 +0000] 94.130.17.190 - B9579B47C8F79A37 '
    'REST.GET.OBJECT edc-s3-mock-data/Cases/ID002 '
    '"GET /edc-data-staging/edc-s3-mock-data/Cases/ID002 HTTP/1.1" '
    '200 - 1234 1234 16 15 "-" "python-requests/2.21.0" - '
    'PwohJhVIBLoSRUrr+D9ElHt/av4XzO86zvIxp27mb5fFMWmKp0uon8DiSFHn63vmSalkc4ZWDdY= - '
    'ECDHE-RSA-AES128-GCM-SHA256 - s3.eu-central-1.amazonaws.com TLSv1.2'
)


assert parse_line(sample_line.encode('ascii')) == Record(
    bucket='edc-data-staging',
    operation='REST.GET.OBJECT',
    key='edc-s3-mock-data/Cases/ID002',
    date=datetime(2019, 5, 3, 2, 40, 29))


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
