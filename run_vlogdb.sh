#!/bin/bash
file_name=./workloads/workload1.spec
dbfilename=/home/ming/vlogSSDDir100/testVLogDB
logname=/home/ming/vlogSSDDir100/log
configpath=/home/ming/workspace/kvseparation/bin/config.ini
rm -rf "$dbfilename"
rm -rf "$logname"
./ycsbc -db vlogwbdb -threads 1 -P "$file_name" -dbfilename "$dbfilename"  -configpath "$configpath"
