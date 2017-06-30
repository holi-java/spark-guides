# Spark

Apache Spark是一个快速和通用的集群计算系统.
Spark使用Hadoop的客户端类库进行HDFS和YARN操作.


在高层次上,每个Spark应用由一个运行用户系统并在集群上执行一系列的并行操作的驱动程序组成.

## Resilient Distributed Dataset

RDDs是一组在集群节点之间划分能进性并行操作的**分布式**的容错数据集. 
RDDs可以从Hadoop InputFormats(HDFS)创建或通过转换其他RDD获取.
RDDs支持两种类型的操作
- transformations - 从一个RDD中创建一个新的RDD，例如: `map(...)`. 所有的transformations都是懒式的, 因为他们并没有立即执行计算. 只有当一个action需要将结果返回给驱动程序是transformations才被执行/计算. 
- actions - 在数据集上计算完之后将结果返回给驱动程序,例如: `reduce(...)`.
## Shared Variables

在并行操作中能够使用Spark的共享变量。
默认情况下，当spark将一个并行操作当作不同节点的一组任务运行时，spark将操作中要使用的变量进行复制然后传递给该操作。
但是有时候，我们希望变量能被一组任务或任务与驱动程序之间共享。spark支持两种共享变量：

- broadcast variables - 用于在所有节点上缓存值到内存中。
- accumulators - 只能将值加入到该变量中不能读取, 如：计数器和求和计算.
 