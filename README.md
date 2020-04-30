# STREAMING_KAFKA

How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
Not much, I added the recommended properties for memory size, however, memory was not a constraint.

What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

I found that increaing spark.driver.memory and spark.executor.memory to 2gb wasn't useful. The app does process in an optimal speed in my pov.

**Console and Spark UI outputs can be found in the [Images](https://github.com/Chappie2/STREAMING_KAFKA/tree/master/Images) folder.**
