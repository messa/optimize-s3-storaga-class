#!/usr/bin/env python3

from argparse import ArgumentParser
import gzip
from heapq import merge as heapq_merge
from itertools import count
from logging import getLogger
from pathlib import Path
import sys
from tempfile import TemporaryDirectory


logger = getLogger(Path(__file__).with_suffix('').name)


def main():
    p = ArgumentParser()
    args = p.parse_args()
    setup_logging()
    with TemporaryDirectory(prefix='sort.') as temp_dir:
        temp_dir = Path(temp_dir)
        sort(sys.stdin.buffer, sys.stdout.buffer, temp_dir)


def sort(input_stream, output_stream, temp_dir):
    temp_files = []
    temp_file_counter = count()
    try:
        while True:
            chunk = []
            for line in input_stream:
                assert line.endswith(b'\n')
                chunk.append(line)
                if len(chunk) >= 1000000:
                    break
            if not chunk:
                break
            chunk.sort()
            temp_file_path = temp_dir / '{:06d}.gz'.format(next(temp_file_counter))
            temp_files.append(temp_file_path)
            with gzip.open(temp_file_path, mode='wb') as f:
                for line in chunk:
                    f.write(line)
        # ok, have all chunks
        open_files = []
        for p in temp_files:
            open_files.append(gzip.open(p, mode='rb'))
        try:
            for line in heapq_merge(*open_files):
                output_stream.write(line)
        finally:
            for f in open_files:
                f.close()
        output_stream.flush()
    finally:
        for p in temp_files:
            try:
                p.unlink()
            except Exception as e:
                logger.exception('Failed to unlink temporary file %s: %r', p, e)


def setup_logging():
    from logging import basicConfig, DEBUG
    basicConfig(level=DEBUG)


if __name__ == '__main__':
    main()
