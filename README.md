# Crispy vs. Blink

This repository presents a code submitted with the bachelor thesis: 
*Comparative Evaluation of Profiling-based
Cluster Resource Allocation Approaches for
Batch Data Processing*

## Prerequisites

Installed and built [HiBench Suite](https://github.com/Intel-bigdata/HiBench) with Spark 3.0 and Scala 2.12

Installed Spark v3.0 (with running MasterNode, at least one WorderNode and History Server)

Installed Hadoop v3.2 (with running DataNode and NameNode)

Configured Hibench Suite with taget Spark's MasterNode and HDFS path

## Structure

`sampler` - Java application for the sampling and automatic HiBench configuration

`dateSizePredictor` - Python application for Blink components, modified Crispy cluster size selector and evaluation

`memoryLogs` - Folder with the collected system-wide memory metrics during the Spark runtime with vm-stat (track_memory.sh)

`eventLogs` - Folder contating eventLogs generated by Spark executors during the runtime.

`track_memory.sh` - Shell script for the total used memory parsing of vm_stat metrics that runs every second

## Evaluation

The evaluation was performed through the data analysis that is located in **CRISPY_memoryUsageAnalyzer** and **BLINK_clusterSizeSelector**.

The result of the analysis is written into **blink-eval** and **crispy-eval** files that are combined and tranformed in the **evaluation_crispy_vs_blink**

## Workflow

Below we describe the workflow of our experiment.

### For Crispy metrics evaluation

1. Ensure that the prerequisites are installed and started with configured Hibench Suite.

2. Generate the dataset with HiBench *prepare* script. Generated dataset is saved in the HDFS.

3. Run the java `sampler` application with the pre-selected requirements in the main class

4. Run ```./track-memory.sh kmeans-0.001.log``` in terminal to collect memory metrics that will be saved in the memory_logs folder. Memory usage of the system will be stored every second. At the same time run any spark job with ```./bin/workloads/ml/kmeans/spark/run.sh```. When Spark is finished abrupt track-memory process.

5. The evaluation of the ``memory_logs`` could be found in the **CRISPY_memoryUsageAnalyzer**.


### For Blink metrics evaluation

1. Ensure that the prerequisites are installed and started

2. Generate the dataset with HiBench *prepare* script. Generated dataset is saved in the HDFS

3. Run the java `sampler` application with the pre-selected requirements in the main class

4. Run ```./bin/workloads/ml/kmeans/spark/run.sh```. Collected metrics could be accessed through executor API over http://localhost:18080/. 

5. The predicted memory size of cached datasets and execution memory should be retrieved after the execution of ``python3 blink_components/sizePredictor.py`` and ``python3 blink_components/executionMemPredictor.py``

6. The predicted values are inserted in the **BLINK_clusterSizeSelector**


## Acknowledgement
The code presented in the **CRISPY_memoryUsageAnalyzer** and dataset **arrow_cluster_jobs_(retrieved-from-Crispy).csv** contains the references to the original implementation located in https://github.com/dos-group/crispy.
