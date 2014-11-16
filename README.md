spark-examples
==============
Anagrams, BigramAnalysis, Inverted indices, vihicle usage are stable at this point, all other classes are WIP. SOme of the examples depand on the local files, and I will be fixing these paths for the inputs.

In order to run the examples from local machine,
Make sure you have spark in your path available.
If you want to run code on cluster, configure your node as edge node for the cluster by copying the Hadoop configuration to your box, and setting HADOOP_CONF_DIR to that diectory where the configuration lies.

Spark shell on the cluster can be started by,
 spark-shell --master yarn
 
To run your code on cluster, you need to first create a fat jar by doing,
sbt assembly

Running job from your jar : 
spark-submit --class classToRun --master masterCanBeLocalOrYarn applicationJar appArguments
 
If, spark complains when run with "yarn" mode, saying spark no classes found for yar, or similar, you can compile spark for CDH and YARN  as, 
sbt/sbt -Dhadoop.version=2.5.0-cdh5.2.0 -Pyarn assembly
