import json
import os
import re
from time import sleep

import sh

from s3fsdb import S3FSDB


def wait_for_cluster_to_balance():
    total_nodes = _total_nodes()

    ring_num_partitions, splits = _get_ring_split()
    even_split = ring_num_partitions / total_nodes
    print '  Rebalancing. Expecting {} nodes with ~{} partitions each'.format(
        total_nodes, even_split
    )
    while True:
        ring_num_partitions, splits = _get_ring_split()
        print '  Current partition split:', splits
        if len(splits) == total_nodes:
            # wait until a majority of nodes have ``~even_split`` partitions
            if sum((even_split - 1) <= split <= (even_split + 1) for split in splits) > total_nodes / 2:
                return
            sleep(5)
        else:
            sleep(10)


def _total_nodes():
    num = sh.wc(sh.Command('docker')('ps', '--filter', 'ancestor=hectcastro/riak-cs', '-q'), '-l')
    return int(num)


def _get_ring_split():
    ring_num_partitions, ring_ownership = get_ring_details()
    splits = re.findall(r"\{'riak@\d+.\d+.\d+.\d+',(\d+)\}", ring_ownership)
    return ring_num_partitions, [int(split) for split in splits]


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
