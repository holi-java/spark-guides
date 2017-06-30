# Spark

Apache Spark is a fast and general-purpose cluster computing system.
Spark uses Hadoop’s client libraries for HDFS and YARN.

At a high level, every Spark application consists of a driver program that runs the user’s main function and executes various parallel operations on a cluster.


## Resilient Distributed Dataset

it is a fault-tolerant **distributed** collection of items partitioned across the nodes of the cluster that can be operated on in parallel. 
RDDs can be created from Hadoop InputFormats(HDFS) or by transforming other RDDs.
RDDs support two types of operations:
 
- transformations - which create a new dataset from an existing one, e.g: `map(...)`. all of the transformations are lazy, in that they do not compute their results right away. The transformations are only computed when an action requires a result to be returned to the driver program.  
- actions -which return a value to the driver program after running a computation on the dataset, e.g: `reduce(...)`.

## Shared Variables

Shared Variables can be used in parallel operations.
By default, when Spark runs a function in parallel as a set of tasks on different nodes, it ships a copy of each variable used in the function to each task.
Sometimes, a variable needs to be shared across tasks, or between tasks and the driver program. 

- broadcast variables -  which can be used to cache a value in memory on all nodes.
- accumulators - which are variables that are only “added” to, such as counters and sums.