import os
import random
import sys
from collections import namedtuple
from contextlib import contextmanager
from cStringIO import StringIO
from threading import Lock
from uuid import uuid4

import boto3
from boto3.s3.transfer import S3Transfer, ReadFileChunk
from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.handlers import calculate_md5
from botocore.utils import fix_s3_host

ValidateResult = namedtuple('ValidateResult',
    'total success mismatch s3_not_found fs_not_found')


class NotFound(Exception):
    pass


class S3FSDB(object):

    def __init__(self, data_dir, url, admin_key, admin_secret):
        self.data_dir = data_dir
        self.db = boto3.resource(
            's3',
            endpoint_url=url,
            aws_access_key_id=admin_key,
            aws_secret_access_key=admin_secret,
            config=Config(connect_timeout=2, read_timeout=5)
        )
        # https://github.com/boto/boto3/issues/259
        self.db.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)
        self.buckets = {}

    def put(self, content, identifier, bucket_name):
        osutil = OpenFileOSUtils()
        transfer = S3Transfer(self.db.meta.client, osutil=osutil)
        transfer.upload_file(content, bucket_name, identifier)

    def get(self, identifier, bucket_name):
        with maybe_not_found(throw=NotFound(identifier, bucket_name)):
            resp = self.get_bucket(bucket_name).Object(identifier).get()
        with ClosingContextProxy(resp["Body"]) as stream:
            return stream.read()

    def clear_s3_bucket(self, bucket_name):
        s3_bucket = self.get_bucket(bucket_name)
        deleted = 0
        with maybe_not_found():
            summaries = s3_bucket.objects.all()
            pages = ([{"Key": o.key} for o in page]
                     for page in summaries.pages())
            for objects in pages:
                resp = s3_bucket.delete_objects(Delete={"Objects": objects})
                deleted += len(set(d["Key"] for d in resp.get("Deleted", [])))
        return deleted

    def clear(self, bucket_name):
        s3_deleted = self.clear_s3_bucket(bucket_name)

        path = os.path.join(self.data_dir, bucket_name)
        fs_deleted = 0
        for name in os.listdir(path):
            os.remove(os.path.join(path, name))
            fs_deleted += 1

        return max(s3_deleted, fs_deleted)

    def create_file(self, bucket_name, filename=None, content=None):
        filename = filename or uuid4().hex
        content = content or filename
        self.dot()
        self.put(StringIO(content), filename, bucket_name)

        path = os.path.join(self.data_dir, bucket_name, filename)
        with open(path, 'w') as f:
            f.write(content)
        return filename

    def random_file(self, bucket_name, size, filename=None):
        filename = filename or uuid4().hex
        path = os.path.join(self.data_dir, bucket_name, filename)
        max_chunk = 1024 ** 2
        bytes_remaining = size

        with open(path, 'w+b') as content:
            with open("/dev/urandom", "rb") as urand:
                while True:
                    chunk = min(max_chunk, bytes_remaining)
                    bytes_remaining -= max_chunk
                    if chunk <= 0:
                        break
                    content.write(urand.read(chunk))
            content.seek(0)
            self.put(content, filename, bucket_name)
        return filename

    def dot(self):
        sys.stdout.write('.')
        sys.stdout.flush()

    def validate(self, bucket_name):
        bucket_path = os.path.join(self.data_dir, bucket_name)
        total, success, mismatch, s3_not_found, fs_not_found = 0, 0, 0, 0, 0
        for file_name in os.listdir(bucket_path):
            self.dot()
            total += 1
            try:
                file_path = os.path.join(bucket_path, file_name)
                with open(file_path, "rb") as fh:
                    fs_content = fh.read()
            except IOError:
                fs_not_found += 1
                continue
            try:
                s3_content = self.get(file_name, bucket_name)
                if s3_content != fs_content:
                    mismatch += 1
                else:
                    success += 1
            except NotFound:
                s3_not_found += 1

        return ValidateResult(total, success, mismatch, s3_not_found, fs_not_found)

    def random_read(self, bucket_name):
        bucket_path = os.path.join(self.data_dir, bucket_name)
        random_file = random.choice(os.listdir(bucket_path))
        self.dot()
        self.get(random_file, bucket_name)

    def get_bucket_keys(self, bucket_name):
        bucket = self.get_bucket(bucket_name)
        return [obj.key for obj in bucket.objects.all()]

    def get_buckets(self):
        return [b.name for b in self.db.buckets.all()]

    def get_bucket(self, bucket_name):
        if bucket_name not in self.buckets:
            try:
                self.db.meta.client.head_bucket(Bucket=bucket_name)
            except ClientError as err:
                if not is_not_found(err):
                    raise
                self.db.create_bucket(Bucket=bucket_name)
                os.makedirs(os.path.join(self.data_dir, bucket_name))
            self.buckets[bucket_name] = self.db.Bucket(bucket_name)
        return self.buckets[bucket_name]


def is_not_found(err, not_found_codes=["NoSuchKey", "NoSuchBucket", "404"]):
    return (err.response["Error"]["Code"] in not_found_codes or
        err.response.get("Errors", {}).get("Error", {}).get("Code") in not_found_codes)


@contextmanager
def maybe_not_found(throw=None):
    try:
        yield
    except ClientError as err:
        if not is_not_found(err):
            raise
        if throw is not None:
            raise throw


class ClosingContextProxy(object):

    def __init__(self, obj):
        self.obj = obj

    def __getattr__(self, name):
        return getattr(self.obj, name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.obj.close()


class OpenFileOSUtils(object):

    def get_file_size(self, fileobj):
        if not hasattr(fileobj, 'fileno'):
            pos = fileobj.tell()
            try:
                fileobj.seek(0, os.SEEK_END)
                return fileobj.tell()
            finally:
                fileobj.seek(pos)
        return os.fstat(fileobj.fileno()).st_size

    def open_file_chunk_reader(self, fileobj, start_byte, size, callback):
        full_size = self.get_file_size(fileobj)
        return ReadOpenFileChunk(fileobj, start_byte, size, full_size,
                                 callback, enable_callback=False)

    def open(self, filename, mode):
        raise NotImplementedError

    def remove_file(self, filename):
        raise NotImplementedError

    def rename_file(self, current_filename, new_filename):
        raise NotImplementedError


class ReadOpenFileChunk(ReadFileChunk):

    def __init__(self, fileobj, start_byte, chunk_size, full_file_size, *args, **kw):

        class FakeFile:
            def seek(self, pos):
                pass

        length = min(chunk_size, full_file_size - start_byte)
        self._chunk = OpenFileChunk(fileobj, start_byte, length)
        super(ReadOpenFileChunk, self).__init__(
            FakeFile(), start_byte, chunk_size, full_file_size, *args, **kw)
        assert self._size == length, (self._size, length)

    def __repr__(self):
        return ("<ReadOpenFileChunk {} offset={} length={}>".format(
            self._chunk.file,
            self._start_byte,
            self._size,
            #get_content_md5(self, self._start_byte, self._chunk.file.tell()),
            #get_content_md5(self._chunk.file),
        ))

    def read(self, amount=None):
        data = self._chunk.read(amount)
        if self._callback is not None and self._callback_enabled:
            self._callback(len(data))
        return data

    def seek(self, where):
        old_pos = self._chunk.tell()
        self._chunk.seek(where)
        if self._callback is not None and self._callback_enabled:
            # To also rewind the callback() for an accurate progress report
            self._callback(where - old_pos)

    def tell(self):
        return self._chunk.tell()

    def close(self):
        self._chunk.close()
        self._chunk = None

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()


class OpenFileChunk(object):
    """A thread-safe wrapper for reading from a shared file-like object"""

    init_lock = Lock()
    file_locks = {}

    def __init__(self, fileobj, start_byte, length):
        with self.init_lock:
            try:
                lock, refs = self.file_locks[fileobj]
            except KeyError:
                lock, refs = self.file_locks[fileobj] = (Lock(), set())
            refs.add(self)
        self.lock = lock
        self.file = fileobj
        self.start = self.offset = start_byte
        self.length = length

    def read(self, amount=None):
        with self:
            if amount is None:
                amount = self.length
            amount = min(self.length - self.tell(), amount)
            return self.file.read(amount)

    def seek(self, pos):
        with self:
            self.file.seek(self.start + pos)

    def tell(self):
        return self.offset - self.start

    def __enter__(self):
        self.lock.acquire()
        self.pos = self.file.tell()
        self.file.seek(self.offset)
        assert self.offset - self.start >= 0, (self.start, self.offset)

    def __exit__(self, *exc):
        try:
            self.offset = self.file.tell()
            self.file.seek(self.pos)
            assert self.offset - self.start >= 0, (self.start, self.offset)
            assert self.offset <= self.start + self.length, \
                (self.start, self.length, self.offset)
        finally:
            self.lock.release()

    def close(self):
        try:
            with self.init_lock:
                lock, refs = self.file_locks[self.file]
                refs.remove(self)
                if not refs:
                    self.file_locks.pop(self.file)
        finally:
            self.file = None
            self.lock = None


def get_content_md5(content, offset=0, filepos=0):
    pos = content.tell()
    print "TELL %s (%s, %s)" % (pos, offset, filepos)
    try:
        content.seek(0)
        params = {"body": content, "headers": {}}
        calculate_md5(params)
        return params["headers"]["Content-MD5"]
    finally:
        print "SEEK", content.tell(), "->", pos, params["headers"]["Content-MD5"]
        content.seek(pos)
