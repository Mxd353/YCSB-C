# YCSB-C

Yahoo! Cloud Serving Benchmark in C++, a C++ version of YCSB (https://github.com/brianfrankcooper/YCSB/wiki)

## Quick Start

To build YCSB-C on Ubuntu, for example:

```
$ sudo apt-get install libtbb-dev
$ sudo apt install libboost-all-dev
$ mkdir build
$ cmake ..
$ make
```
run:
```
cd build
./ycsbc -T 2 -P workloads/workloada.spec
```
-T threadcount<br>
-db dbname<br>
-l load(bool)<br>
-ding ding(bool)<br>
-P workloadfile<br>
