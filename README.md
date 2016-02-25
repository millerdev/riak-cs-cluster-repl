# Repl for playing with a Riak CS cluster


## Setup
### Build docker image
```
git clone https://github.com/snopoke/docker-riak-cs.git
cd docker-riak-cs
git checkout patch-1
make build
```

### Setup repl
```
git clone https://github.com/snopoke/riak-cs-cluster-repl.git
cd riak-cs-cluster-repl
mkvirtuanenv riak-repl
pip install -r requirements.txt
```

## Running REPL
```
$ python runner.py
Raik Testing Tool
=> help

Documented commands (type help <topic>):
========================================
EOF        get               list_nodes   riak_admin      stop_node        
add_node   help              put          ring_ownership  validate_data    
add_nodes  list_bucket_keys  remove_node  shell           wait             
exit       list_buckets      reset        start_node      write_random_data

=> 
```

## Run Script
```
$ cat script
reset
add_nodes 3
write_random_data bucket-a 100
validate_data bucket-a

stop_node 03
validate_data bucket-a

$ python runner.py script
```

See [example_script.txt](example_script.txt) for a more realistic example.

## Real testing
To really test the cluster it's useful to run 2 REPL sessions concurrently.
Here is an example session that tests removing a node from a 3 node cluster:

```
repl1 => reset
repl1 => add_nodes 3
repl1 => write_random_data bucket-a 1000

repl2 => validate_data_continuous bucket-a
  Validating bucket bucket-a
..................................

repl1 => stop_node 03
```
