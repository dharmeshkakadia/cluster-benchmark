cluster-benchmark
=================

##How to use

1. Download it.
  ```
  git clone https://github.com/dharmeshkakadia/cluster-benchmark && cd cluster-benchmark
  ```

2. Compile it.
  ```
  mvn package
  ```
 
3. Run it.
  ```
  hadoop jar target/cluster-benchmark.jar [BenchmakrName]
  ```

Currently following benchmarks are available:

* ``Sleep``: Streeses YARN components (RM,NM,Historyserver) etc by creating a job with given number of tasks which sleeps for given amount of time.

* ``Compute``: Measures the compute capacity available on the cluster with a job which repeatedly generaetes random numbers. 

* ``Read``: Measures the read capactiy of the file system.

* ``DFSWrite``: Measures the write capacity of the distributed file system (WASB,HDFS,ADLS, etc.)

* ``Write``: Measures the write capacity of local disk writes.

* ``Shuffle``: Measures the shuffle performance of the cluster.


* ``ReadCompute``: Generates a job that does given amount of read and compute.

* ``ReadComputeWrite``: Generates a job that does given amount of read, compute and write.

* ``Mix``: Generates a job which generates a mix of compute,read and write.

* ``Mix2``: Genereates a job which generates a mix of compute,read and write.
