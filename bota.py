#!/usr/bin/env python
#
# Author: Miroslav Bauer @ CESNET 2017 <bauer@cesnet.cz>
#
# BOToArchiver is a simple tool for managing objects in Amazon S3 storage based on boto3 library.
# It allows for making and removing "buckets" and uploading, downloading and removing
# "objects" from these buckets
#
import os
import sys
import time
import boto3
import errno
import argparse
import threading
from urllib import unquote
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        try:
            self._size = float(os.path.getsize(filename))
        except OSError:
            self._size = -1
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def get_size(self):
        return self._size if self._size > 0 else self._seen_so_far

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self.get_size()) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self.get_size(),
                    percentage))
            sys.stdout.flush()


def get_s3_client(host):
    if host.startswith('http'):
        endpoint = host
    else:
        endpoint = "https://" + host + "/"
    return boto3.client('s3', endpoint_url=endpoint)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as oe:
        if oe.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def lsb(args):
    s3 = get_s3_client(args.host)
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        print bucket['Name']


def ls(args):
    # TODO: implement detailed listing, similar to ls -l format

    s3 = get_s3_client(args.host)
    response = s3.list_objects_v2(
        Bucket=args.bucket,
        EncodingType='url',
        MaxKeys=args.limit,
        Prefix=args.prefix,
        StartAfter=args.offset,
    )
    try:
        for content in response['Contents']:
            print unquote(content['Key'])
    except KeyError:
        print '> [WARN] There are no objects in a bucket "%s"' % args.bucket
        return

    return map(lambda c: unquote(c['Key']), response['Contents'])


def put(args):
    # TODO: implement ACLs
    # TODO: implement uploading from file-like objects
    # TODO: impleament extra args

    s3 = get_s3_client(args.host)
    filelist = []
    if os.path.isdir(args.filepath):
        for root, dirnames, filenames in os.walk(args.filepath):
            for fn in filenames:
                fpath = os.path.join(root, fn)
                filelist.append({'src': fpath, 'tgt': fpath.strip('/')})
    elif os.path.isfile(args.filepath):
        objname = args.objname.strip('/') if args.objname else args.filepath.strip('/')
        filelist.append({'src': args.filepath, 'tgt': objname})
    else:
        print '> [ERROR] Only file or directory uploads are supported'

    for obj in filelist:
        print '> Uploading %s to %s%s/%s' % (obj['src'], str(s3._endpoint).lstrip('s3(').rstrip(')'),
                                             args.bucket, obj['tgt'])
        if args.progress:
            pp_callback = ProgressPercentage(obj['src'])
            ts_upload_start = time.time()
        else:
            pp_callback = None
        try:
            s3.upload_file(obj['src'], args.bucket, obj['tgt'], Callback=pp_callback)
        except OSError as e:
            print '> [ERROR]: %s: %s' % (e.strerror, e.filename)
        except S3UploadFailedError as se:
            if 'NoSuchBucket' in se.message:
                print '> [ERROR]: Bucket "%s" doesn\'t exist' % args.bucket
            else:
                raise
        if args.progress:
            elapsed = time.time()-ts_upload_start
            print '\n> Upload complete. Average speed: %d B/s, time elapsed: %d s' %\
                  (int(pp_callback.get_size())/elapsed, elapsed)


def get(args):
    # TODO: implement extra args

    s3 = get_s3_client(args.host)
    objlist = []

    if args.objname.endswith('/'):
        if args.filepath and (not args.filepath.endswith('/') or not os.path.isdir(args.filepath)):
            print '> [ERROR] When downloading a directory, target path must also be a directory'
            return
        args.prefix = args.objname
        args.limit = sys.maxsize - 50000000000  # experimental decrement, maxsize-1 somehow returns nothing o_O
        args.offset = ''
        dirlist = ls(args)
        if not dirlist:
            print '> [ERROR]: Source directory "%s" doesn\'t exist or is empty' % args.objname
            return
        if not args.filepath:
            args.filepath = '/'
        objlist = [{'src': f, 'tgt': args.filepath + f} for f in dirlist]
    else:
        tgt = args.objname.split('/')[-1] if not args.filepath else args.filepath
        if tgt.endswith('/'):
            tgt += args.objname.split('/')[-1]
            args.filepath = tgt
        objlist.append({'src': args.objname, 'tgt': tgt})

    for obj in objlist:
        if args.progress:
            pp_callback = ProgressPercentage(obj['tgt'])
            ts_download_start = time.time()
        else:
            pp_callback = None
        if args.makedirs:
            mkdir_p(os.path.dirname(obj['tgt']))
        print '> Downloading %s%s/%s to %s' % (str(s3._endpoint).lstrip('s3(').rstrip(')'),
                                               args.bucket, obj['src'], obj['tgt'])
        try:
            s3.download_file(args.bucket, obj['src'], obj['tgt'], Callback=pp_callback)
        except ClientError as ce:
            if 'Not Found' in ce.message:
                print '> [ERROR]: Object "%s" not found in bucket %s' % (obj['src'], obj['tgt'])
            else:
                raise
        except IOError as ie:
            if ie.errno == errno.ENOENT:
                print '> [ERROR]: Parent folder %s doesn\'t exist' % os.path.dirname(obj['tgt'])
            else:
                raise
        if args.progress:
            elapsed = time.time()-ts_download_start
            print '\n> Download complete. Average speed: %d B/s, time elapsed: %d s' %\
                  (int(pp_callback.get_size())/elapsed, elapsed)


def parse_args():
    # Parse general arguments
    parser = argparse.ArgumentParser(description='''Bota is a tool for managing objects in Amazon S3 storage.
                                        It allows for making and removing "buckets" and uploading, downloading
                                        and removing "objects" from these buckets.''')
    parser.add_argument('host', metavar='S3_HOSTNAME', help='S3 endpoint hostname')
    subparsers = parser.add_subparsers(title='S3 commands')

    # Parse LSB command arguments
    parser_lsb = subparsers.add_parser('lsb', help='List all available buckets')
    parser_lsb.set_defaults(func=lsb)

    # Parse LS command arguments
    parser_ls = subparsers.add_parser('ls', help='List objects in a bucket')
    parser_ls.add_argument('bucket', help='Target S3 bucket name')
    parser_ls.add_argument('-l', '--limit', dest='limit',
                           help='Maximum number of objects in response (default: 1000)', type=int, default=1000)
    parser_ls.add_argument('-p', '--prefix', dest='prefix',
                           help='Limits the response to keys that begin with the specified prefix.', default='')
    parser_ls.add_argument('-o', '--offset', dest='offset',
                           help='Starts listing after this specified object key', default='')
    parser_ls.set_defaults(func=ls)

    # Parse PUT command arguments
    parser_put = subparsers.add_parser('put', help='upload a file')
    parser_put.add_argument('filepath', help='Path of source file to be uploaded')
    parser_put.add_argument('bucket', help='Target S3 bucket name')
    parser_put.add_argument('-o', dest='objname', metavar='OBJNAME',
                            help='''Target S3 object name.
                                 Applies only to a single file uploads (default: source filepath)''')
    parser_put.add_argument('--progress', dest='progress', action='store_true',
                            help='Show transfer progress')
    parser_put.set_defaults(func=put)

    # Parse GET command arguments
    parser_get = subparsers.add_parser('get', help='download a file')
    parser_get.add_argument('bucket', help='Source S3 bucket name')
    parser_get.add_argument('objname', help='Source S3 object name')
    parser_get.add_argument('-f', dest='filepath', metavar='FILEPATH',
                            help='Target filename (default: object name)')
    parser_get.add_argument('-d', '--make-dirs', dest='makedirs', action='store_true',
                            help='Autocreate missing parent directories')
    parser_get.add_argument('--progress', dest='progress', action='store_true',
                            help='Show transfer progress')
    parser_get.set_defaults(func=get)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    parse_args()
