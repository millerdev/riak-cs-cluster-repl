import json
import os
import re
from time import sleep

import sh

from s3fsdb import S3FSDB


def wait_for_cluster_to_balance():
    total_nodes = _total_nodes()

    while True:
        ring_num_partitions, splits = _get_ring_split()
        print '  Waiting for rebalance. Current ring split:', splits
        if len(splits) == total_nodes:
            min_allowed = ring_num_partitions / total_nodes / 4 * 3
            if all(split > min_allowed for split in splits):
                return
            sleep(2)
        else:
            sleep(5)


def _total_nodes():
    num = sh.wc(sh.Command('docker')('ps', '--filter', 'ancestor=hectcastro/riak-cs', '-q'), '-l')
    return int(num)


def _get_ring_split():
    ring_num_partitions, ring_ownership = get_ring_details()
    splits = re.findall(r"\{'riak@\d+.\d+.\d+.\d+',(\d+)\}", ring_ownership)
    return ring_num_partitions, splits


def get_ring_details():
    status = sh.Command('./bin/get_stats.sh')()
    status = json.loads(str(status))
    ring_num_partitions = status['ring_num_partitions']
    ring_ownership = status['ring_ownership']
    return ring_num_partitions, ring_ownership


def _get_riak_config(config):
    if not os.path.isfile(config.riak_config_path):
        raise Exception('Config file not found', config.riak_config_path)

    with open(config.riak_config_path, 'r') as f:
        return json.load(f)


_db = None
def get_db(config):
    global _db
    if not _db:
        riak_config = _get_riak_config(config)
        _db = S3FSDB(config.data_dir, **riak_config)
    return _db
