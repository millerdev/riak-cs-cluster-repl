import os
import random
import sys
from collections import namedtuple
from contextlib import contextmanager
from uuid import uuid4

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.utils import fix_s3_host

ValidateResult = namedtuple('ValidateResult', 'total success not_found mismatch')

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
            config=Config(connect_timeout=1, read_timeout=1)
        )
        # https://github.com/boto/boto3/issues/259
        self.db.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)
        self.buckets = {}

    def put(self, content, identifier, bucket_name):
        self.get_bucket(bucket_name).put_object(
            Key=identifier,
            Body=content,
        )

    def get(self, identifier, bucket_name):
        with maybe_not_found(throw=NotFound(identifier, bucket_name)):
            resp = self.get_bucket(bucket_name).Object(identifier).get()
        with ClosingContextProxy(resp["Body"]) as stream:
            return stream.read()

    def create_file(self, bucket_name, filename=None, content=None):
        filename = filename or uuid4().hex
        content = content or filename
        self.dot()
        self.put(filename, content, bucket_name)

        path = os.path.join(self.data_dir, bucket_name, filename)
        with open(path, 'w') as f:
            f.write(content)
        return filename

    def dot(self):
        sys.stdout.write('.')
        sys.stdout.flush()

    def validate(self, bucket_name):
        bucket_path = os.path.join(self.data_dir, bucket_name)
        total, success, not_found, mismatch = 0, 0, 0, 0
        for file_name in os.listdir(bucket_path):
            self.dot()
            total += 1
            try:
                content = self.get(file_name, bucket_name)
                if not content == file_name:
                    mismatch += 1
                else:
                    success += 1
            except NotFound:
                not_found += 1

        return ValidateResult(total, success, not_found, mismatch)

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
