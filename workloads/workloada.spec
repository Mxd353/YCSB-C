# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian
#
#   Zipfian constant (skewness) configuration:
#   - zipfian_const=0.99 (default): High skew, ~70% requests to ~1% hot keys
#   - zipfian_const=0.8: Moderate skew, more distributed access
#   - zipfian_const=0.0: Uniform distribution, no skew

recordcount=50
operationcount=50
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0

requestdistribution=zipfian
# zipfian_const=0.99

