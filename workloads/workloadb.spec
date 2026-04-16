# Yahoo! Cloud System Benchmark
# Workload B: Read mostly workload
#   Application example: photo tagging; add a tag is an update, but most operations are to read tags
#
#   Read/update ratio: 95/5
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian
#
#   Zipfian constant (skewness) configuration:
#   - zipfian_const=0.99 (default): High skew, ~70% requests to ~1% hot keys
#   - zipfian_const=0.8: Moderate skew, more distributed access
#   - zipfian_const=0.0: Uniform distribution, no skew

recordcount=30000
operationcount=1000000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0.95
updateproportion=0.05
scanproportion=0
insertproportion=0

requestdistribution=zipfian
# zipfian_const=0.99
