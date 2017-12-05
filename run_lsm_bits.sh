#!/bin/bash
experiment_time=6
DISK=SSD"$experiment_time"
dbfilename_o=/home/ming/RAID0_"$DISK"/hlsm
function __loadLSM(){
    rm -rf "$dbfilename"
    loadname=$1
    loadname="$loadname"_load.txt
    runname=$1
    runname="$runname"_run
    dirname=$2
    levelIn=$3
    ltype=$4
    bb=$5
    workloadw_name=./workloads/glsmworkloadw_"$levelIn"_"$sizeRatio".spec
    if [ ! -d "$dirname" ]; then
	mkdir -p "$dirname"
    fi

    echo ./ycsbc -db leveldb -threads 1 -P $workloadw_name -dbfilename "$dbfilename" -configpath "$configpath" -skipLoad false #> "$loadname"
    sync;echo 1 > /proc/sys/vm/drop_caches
    sleep 100s
    mv "$loadname" "$dirname"
}

function __runLSM(){
    loadname=$1
    runname=$1
    runname="$runname"_run
    dirname=$2
    levelIn=$3
    ltype=$4
    bb=$5
    __modifyConfig directIOFlag true
    workloadr_name=./workloads/glsmworkloadr_"$levelIn"_"$sizeRatio".spec
    if [ ! -d "$dirname" ]; then
	mkdir -p "$dirname"
    fi
    for j in `seq 1 2`
    do
	./ycsbc -db leveldb -threads 1 -P $workloadr_name -dbfilename "$dbfilename" -configpath "$configpath" -skipLoad true -requestdistribution "$requestdistribution" -zipfianconst "$zipfianconst" > "$runname"_"$j".txt
	sync;echo 1 > /proc/sys/vm/drop_caches
	mv "$runname"_"$j".txt "$dirname"
	mv testlf.txt "$dirname"/latency_l"$levelIn"_lsmtype_"$ltype"_bloom_"$bb"_"$j".txt
	mv nlf.txt "$dirname"/nlatency_l"$levelIn"_lsmtype_"$ltype"_bloom_"$bb"_"$j".txt
        sleep 100s
    done
}


configpath=./configDir/leveldb_config.ini
section=basic
function __modifyConfig(){
    key=$1
    value=$2
    ./modifyConfig.py "$configpath" "$section" "$key" "$value"   
}

leveldb_dir=/home/ming/workspace/leveldb_hierarchical

#run lsm
function __checkOutBranch(){
    branch=$1
    cd "$leveldb_dir"
    git checkout "$branch"
    ./install_leveldb.sh
    cd -
    make
}
#branches=(lsm)
types=(lsm-hierarchical)
bloom_bit_array=(2)
level=6
sizeRatio=2
requestdistribution=zipfian
zipfianconst=0.99
for lsmtype in ${types[@]}
do
#    __checkOutBranch NoSeekCompaction
    __modifyConfig hierarchicalBloomflag true
    for bloombits in ${bloom_bit_array[@]}
    do
	echo bloombits:"$bloombits"
	__modifyConfig bloomBits  "$bloombits"
	__modifyConfig sizeRatio  "$sizeRatio"
	echo level:"$level"
	dbfilename="$dbfilename_o"_l"$level"_b"$bloombits"_s"$sizeRatio"
	if [ "$lsmtype" = "lsm" ]; then
	    echo "lsm"
	    __modifyConfig bloomFilename /home/ming/workspace/blooml"$level"_"$bloombits".txt
	else
	    echo "lsm-hierarchical"
	    __modifyConfig bloomFilename /home/ming/workspace/blooml"$level"_"$bloombits"_h1.txt
	fi
	#__loadLSM bloombits"$bloombits"_level"$level"_lsmtype_"$lsmtype" /home/ming/workspace/YCSB-C/lsm_"$DISK"_read/skipratio2_zip1.2_sizeRatio"$sizeRatio" "$level"  "$lsmtype" "$bloombits"
	if [ "$requestdistribution" = "zipfian" ]; then
	    echo "zipfian"
	    __runLSM bloombits"$bloombits"_level"$level"_lsmtype_"$lsmtype" /home/ming/workspace/YCSB-C/lsm_"$DISK"_read/skipratio2_"$requestdistribution""$zipfianconst"_sizeRatio"$sizeRatio" "$level"  "$lsmtype" "$bloombits"
	else
	    echo "$requestdistribution"
	    __runLSM bloombits"$bloombits"_level"$level"_lsmtype_"$lsmtype" /home/ming/workspace/YCSB-C/lsm_"$DISK"_read/skipratio2_"$requestdistribution"_sizeRatio"$sizeRatio" "$level"  "$lsmtype" "$bloombits"
	fi

    done
done

#__runGLSM


