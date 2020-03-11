#!/usr/bin/env python3

from argparse import ArgumentParser
from collections import defaultdict
import sys


def main():
    p = ArgumentParser()
    args = p.parse_args()
    total_usage_by_bucket = defaultdict(Directory)
    process_stream(total_usage_by_bucket, sys.stdin.buffer)
    for bucket_name, root_directory in sorted(total_usage_by_bucket.items()):
        root_directory.print_usage(bucket_name)


class Directory:

    subdir_count_limit = 50

    def __init__(self):
        self.total_bytes = defaultdict(lambda: 0)
        self.subdirectories = defaultdict(Directory)

    def __repr__(self):
        return '<{cls} total_bytes={s.total_bytes} subdirectories={s.subdirectories}>'.format(
            cls=self.__class__.__name__, s=self)

    def update(self, key_parts, storage_class, size):
        assert isinstance(key_parts, list)
        self.total_bytes[storage_class] += size
        if key_parts and len(self.subdirectories) < self.subdir_count_limit:
            subdir, *rest = key_parts
            self.subdirectories[subdir].update(rest, storage_class, size)

    def print_usage(self, bucket_name, key=''):
        for sc_name, sc_bytes in sorted(self.total_bytes.items()):
            print(bucket_name, key.ljust(60), sc_name, nice_bytes(sc_bytes))
        if len(self.subdirectories) < self.subdir_count_limit:
            for subdir_name, subdir in sorted(self.subdirectories.items()):
                subdir_key = (key + '/' + subdir_name).lstrip('/')
                subdir.print_usage(bucket_name, subdir_key)


def nice_bytes(v):
    return '{:9.2f} GB'.format(v / 2**30)


def process_stream(total_usage_by_bucket, stream):
    for line in stream:
        assert isinstance(line, bytes)
        line = line.decode('UTF-8')
        bucket_name, key, size, storage_class = line.split()
        size = int(size)
        total_usage_by_bucket[bucket_name].update(key.split('/')[:-1], storage_class, size)


if __name__ == '__main__':
    main()
