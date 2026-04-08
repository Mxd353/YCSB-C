# Yahoo! Cloud System Benchmark
# Workload C: Read only
#   Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
#
#   Read/update ratio: 100/0
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian
#
#   Zipfian constant (skewness) configuration:
#   - zipfian_const=0.99 (default): High skew, ~70% requests to ~1% hot keys
#   - zipfian_const=0.8: Moderate skew, more distributed access
#   - zipfian_const=0.0: Uniform distribution, no skew

recordcount=100000
operationcount=100000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0

requestdistribution=zipfian
# zipfian_const=0.99



