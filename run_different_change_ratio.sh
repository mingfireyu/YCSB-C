#!/bin/bash
experiment_time=6
DISK=SSD"$experiment_time"
dbfilename=/home/ming/RAID0_"$DISK"/mlsm
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
    if [ ! -d "$dirname" ]; then
	mkdir  -p "$dirname"
    fi
    __modifyConfig directIOFlag false
    ./ycsbc -db leveldb -threads 1 -P $workloadw_name -dbfilename "$dbfilename" -configpath "$configpath" -skipLoad false > "$loadname"
    sync;echo 1 > /proc/sys/vm/drop_caches
    sleep 100s
    mv "$loadname" "$dirname"
}

function __runLSM(){
    runname=$1
    runname="$runname"
    dirname=$2
    levelIn=$3
    ltype=$4
    bb=$5
    cR=$6
    workloadr_name=./workloads/glsmworkloadr_"$levelIn".spec
    life_times=(10000 20000 30000)
    __modifyConfig directIOFlag true
    section=LRU
    if [ ! -d "$dirname" ]; then
	mkdir  -p "$dirname"
    fi
    section=LRU
    for life_time in ${life_times[@]}
    do
	echo life_time "$life_time"
	__modifyConfig LifeTime "$life_time"
        ./ycsbc -db leveldb -threads 1 -P $workloadr_name -dbfilename "$dbfilename" -configpath "$configpath" -skipLoad true > "$runname"_changeRatio"$cR"_lifetime"$life_time".txt
	sync;echo 1 > /proc/sys/vm/drop_caches
	mv "$runname"_changeRatio"$cR"_lifetime"$life_time".txt "$dirname"/
	mv testlf1.txt "$dirname"/latency_l"$levelIn"_lsmtype_"$ltype"_bloom_"$bb"_changeRatio"$cR"_lifetime"$life_time".txt
	mv nlf1.txt "$dirname"/nlatency_l"$levelIn"_lsmtype_"$ltype"_bloom_"$bb"_changeRatio"$cR"_lifetime"$life_time".txt
	mv level?_access_frequencies.txt "$dirname"/
        sleep 100s
    done
    cp configDir/leveldb_config.ini "$dirname"/
}


lsmtype=(lsm)
bloombits=6
level=6
dbfilename="$dbfilename""$level"
FilterCapacityRatios=(6.0 5.0)
changeRatios=(0.01 0.001 0.0001)

for FilterCapacityRatio in ${FilterCapacityRatios[@]}
do
    __modifyConfig bloomType 2
    __modifyConfig seekCompactionFlag false
    echo Counterpart bloombits:"$bloombits"
    __modifyConfig bloomBits  "$bloombits"
    section=LRU
    __modifyConfig FilterCapacityRatio "$FilterCapacityRatio"
    section=basic
    for changeRatio in ${changeRatios[@]}
    do
	section=LRU
	__modifyConfig changeRatio "$changeRatio"
	section=basic
	dirname=/home/ming/experiment/expectation/lsm_"$DISK"_read_zipfian0.99_multi_filter/experiment"$experiment_time"/BGShrinkUsage_FilterCapacityRatio_"$FilterCapacityRatio"_lru0_300WRead
	#__loadLSM bloombits"$bloombits"_level"$level"_lsmtype_"$lsmtype" "$dirname" "$level"  "$lsmtype" "$bloombits"
	__runLSM bloombits"$bloombits"_level"$level"_lsmtype_"$lsmtype" "$dirname" "$level"  "$lsmtype" "$bloombits" "$changeRatio"
    done
done

#__runGLSM


