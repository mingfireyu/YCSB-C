# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=3221225472
#operationcount=1211034
operationcount=3000000
#operationcount=1000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=1.0
updateproportion=0.0
scanproportion=0
insertproportion=0
fieldlength=76
requestdistribution=zipfian

withtimestamp=false
timestamptracefilename=trace2.txt
withoperation=false

withlatency=true
latencyfilename=testlf
adjustfilter=false
