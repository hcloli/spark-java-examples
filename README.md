# spark-java-examples
Since I feel most of Apache Spark examples on the internet exist in Scala, I decided to put some Java examples to let people writing Spark in Java an idea how to use this API.

The examples include:
## Simple log parsing
This example shows how to parse log file, extract text and count occurences of this text.

File: [spark-java-examples/src/main/java/com/haimcohen/spark/SimpleSparkTest.java](https://github.com/hcloli/spark-java-examples/blob/master/src/main/java/com/haimcohen/spark/SimpleSparkTest.java)

## SPark SQL User Defined Function (UDF)
Spark SQL allow users to define their own functions to create Columns. This example show how to do it.

File: [spark-java-examples/src/main/java/com/haimcohen/spark/SparkSqlAddUUIDColumn.java](https://github.com/hcloli/spark-java-examples/blob/master/src/main/java/com/haimcohen/spark/SparkSqlAddUUIDColumn.java)
