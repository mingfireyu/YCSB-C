#!/bin/bash
file_name=./workloads/workloadt.spec
./ycsbc -db vlogwbdb -threads 1 -P $file_name -dbfilename "testVLogDB" -configpath "/home/ming/workspace/config.ini"