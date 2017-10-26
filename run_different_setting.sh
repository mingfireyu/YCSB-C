#!/bin/bash
DISK=HDD
dbfilename=/home/ming/vlog"$DISK"Dir10/lsm
configpath=./configDir/leveldb_config.ini
section=basic

function __modifyConfig(){
    key=$1
    value=$2
    ./modifyConfig.py "$configpath" "$section" "$key" "$value"   
}

function __loadLSM(){
    rm -rf "$dbfilename"
    loadname=$1
    loadname="$loadname"_load.txt
    dirname=$2
    levelIn=$3
    ltype=$4
    bb=$5
    workloadw_name=./workloads/glsmworkloadw_"$levelIn".spec
    __modifyConfig directIOFlag false
    ./ycsbc -db leveldb -threads 1 -P $workloadw_name -dbfilename "$dbfilename" -configpath "$configpath" -skipLoad false > "$loadname"
    sync;echo 1 > /proc/sys/vm/drop_caches
    sleep 100s
    if [ ! -d "$dirname" ]; then
	mkdir  -p "$dirname"
    fi
    mv "$loadname" "$dirname"
}

function __runLSM(){
    runname=$1
    runname="$runname"_run
    dirname=$2
    levelIn=$3
    ltype=$4
    bb=$5
    workloadr_name=./workloads/glsmworkloadr_"$levelIn".spec
    base_nums=(8 16  32 64)
    life_times=(10 20 30 40 50)
    __modifyConfig directIOFlag false
     for base_num in ${base_nums[@]}
     do
	echo Base_Num "$base_num"
	section=LRU
	__modifyConfig BaseNum "$base_num"
        ./ycsbc -db leveldb -threads 1 -P $workloadr_name -dbfilename "$dbfilename" -configpath "$configpath" -skipLoad true > "$runname"_base"$base_num".txt
	sync;echo 1 > /proc/sys/vm/drop_caches
	mv "$runname"_"$j".txt "$dirname"
	mv testlf1.txt "$dirname"/latency_l"$levelIn"_lsmtype_"$ltype"_bloom_"$bb"_"$j"_noseek_fix"$j".txt
	mv level?_filter_count_?.txt "$dirname"/
        sleep 100s
    done

}


types=(lsm)
bloom_bit_array=(8)
level=4
for lsmtype in ${types[@]}
do
    __modifyConfig bloomType 2
    __modifyConfig seekCompactionFlag false
    for bloombits in ${bloom_bit_array[@]}
    do
	echo Counterpart bloombits:"$bloombits"
	__modifyConfig bloomBits  "$bloombits"
	dirname=/home/ming/workspace/YCSB-C/lsm_"$DISK"_read_zipfian/Base_num_test
	#__loadLSM bloombits"$bloombits"_level"$level"_lsmtype_"$lsmtype" "$dirname" "$level"  "$lsmtype" "$bloombits"
	__runLSM bloombits"$bloombits"_level"$level"_lsmtype_"$lsmtype" "$dirname" "$level"  "$lsmtype" "$bloombits"
    done
done

#__runGLSM


