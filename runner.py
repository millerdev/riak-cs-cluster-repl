from __future__ import print_function

import sys
from collections import namedtuple

from repl import RiakTester, AutoRiakTester

Command = namedtuple('Command', 'name args')
Config = namedtuple('Config', 'data_dir default_bucket riak_config_path')
CONFIG = Config('test_data', 'default', 'config.json')

if __name__ == '__main__':
    if len(sys.argv) > 1:
        input = open(sys.argv[1], 'rt')
        try:
            AutoRiakTester(CONFIG, stdin=input).cmdloop()
        finally:
            input.close()
    else:
        RiakTester(CONFIG).cmdloop()
