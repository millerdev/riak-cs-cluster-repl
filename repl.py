import cmd
import os
import shutil
import sys
from time import sleep

import sh

from s3fsdb import NotFound
from utils import wait_for_cluster_to_balance, get_db, get_ring_details

docker = sh.Command('docker')

class RiakTester(cmd.Cmd):
    prompt = '=> '
    intro = "Raik Testing Tool"

    def __init__(self, config, stdin=None):
        cmd.Cmd.__init__(self, stdin=stdin)
        self.config = config

    def do_exit(self, args):
        """Exits from the console"""
        return True

    def do_EOF(self, args):
        """Exit on system end of file character"""
        return self.do_exit(args)

    def do_shell(self, args):
        """Pass command to a system shell when line begins with '!'"""
        os.system(args)

    def default(self, line):
        if line and line[0] == '#':
            return

        cmd.Cmd.default(self, line)

    def emptyline(self):
        return

    def do_remove_node(self, node_index):
        """remove_node [node index]"""
        docker('rm', '-fv', 'riak-cs{}'.format(node_index))

    def do_stop_node(self, node_index):
        """stop_node [node index]"""
        docker('stop', 'riak-cs{}'.format(node_index))
        sleep(5)

    def do_start_node(self, node_index):
        """start_node [node index]"""
        docker('start', 'riak-cs{}'.format(node_index))
        sleep(5)

    def do_list_nodes(self, args):
        """list all the nodes (running and not running)"""
        print docker(
            'ps', '-a', '--filter', 'ancestor=hectcastro/riak-cs', '--format', '"{{.Names}}: {{.Status}}"'
        )

    def do_riak_admin(self, args):
        """riak_admin [admin command]
        Run with no command to list the available commands"""
        args = args.split(' ')
        print sh.Command('./bin/ssh_command.sh')('riak-admin', *args, _ok_code=[0,1])

    def do_ring_ownership(self, args):
        """Print the ring ownership"""
        ring_num_partitions, ring_ownership = get_ring_details()
        print ring_ownership

    def do_reset(self, args):
        """reset
        Delete all nodes and data
        """
        if os.path.exists(self.config.data_dir):
            shutil.rmtree(self.config.data_dir)

        sh.Command('./bin/kill-cluster.sh')()
        if os.path.exists(self.config.riak_config_path):
            os.remove(self.config.riak_config_path)

    def do_add_node(self, args):
        """add_node
        Add a node to the cluster and wait for it to come up and for the
        cluster to stabalize"""
        sh.Command('./bin/add_node.sh')()
        wait_for_cluster_to_balance()

    def do_add_nodes(self, num):
        """add_nodes [N]"""
        try:
            num = int(num)
        except:
            cmd.Cmd.do_help('add_nodes')
            return

        for i in range(num):
            print '  Adding node', i + 1
            sh.Command('./bin/add_node.sh')()
        wait_for_cluster_to_balance()

    def do_wait_for_rebalance(self, args):
        wait_for_cluster_to_balance()

    def do_write_random_data(self, args):
        """write_random_data [bucket] [num files]"""
        try:
            bucket, num_files = args.split(' ')
        except:
            self.do_help('write_random_data')
            return

        db = get_db(self.config)
        db.get_bucket(bucket)
        for i in range(int(num_files)):
            db.create_file(bucket)
        print

    def do_put(self, args):
        """put [bucket] [key] [contents]"""
        try:
            bucket, key, content = args.split(' ')
        except:
            self.do_help('put')
            return

        db = get_db(self.config)
        db.get_bucket(bucket)
        print db.create_file(bucket, key, content)

    def do_get(self, args):
        """get [bucket] [key]"""
        try:
            bucket, key = args.split(' ')
        except:
            self.do_help('get')
            return

        db = get_db(self.config)
        try:
            data = db.get(key, bucket)
        except NotFound:
            print '{}/{} not found'.format(bucket, key)
        else:
            print '-' * 100
            print data
            print '-' * 100

    def do_list_bucket_keys(self, bucket_name):
        """list_bucket_keys [bucket]"""
        db = get_db(self.config)
        try:
            keys = db.get_bucket_keys(bucket_name)
        except Exception as e:
            print e
        else:
            for key in keys:
                print key

    def do_list_buckets(self, args):
        db = get_db(self.config)
        try:
            buckets = db.get_buckets()
        except Exception as e:
            print e
        else:
            for bucket in buckets:
                print bucket

    def do_validate_data(self, buckets):
        """validate_data [bucket]
        Read all the data in a bucket and check that it matches what we
        have stored on disk"""
        if not buckets:
            self.do_help('validate_data')
            return

        buckets = buckets.split(' ')
        db = get_db(self.config)
        for bucket in buckets:
            print '  Validating bucket', bucket
            try:
                results = db.validate(bucket)
            except Exception as e:
                print e
            else:
                print
                if results.success != results.total:
                    print "  Validating error: ", results

    def do_validate_data_continuous(self, buckets):
        """validate_data_continuous [buckets]"""
        print 'Press Ctrl-C to stop'
        try:
            while True:
                self.do_validate_data(buckets)
        except KeyboardInterrupt:
            print
            return

    def do_wait(self, args):
        """wait [N seconds]"""
        for i in range(int(args)):
            sys.stdout.write('.')
            sys.stdout.flush()
            sleep(1)
        print


class AutoRiakTester(RiakTester):
    # Disable rawinput module use
    use_rawinput = False

    # Do not show a prompt after each command read
    prompt = ''

    def precmd(self, line):
        if line and line[0] != '#':
            print line
        return cmd.Cmd.precmd(self, line)
