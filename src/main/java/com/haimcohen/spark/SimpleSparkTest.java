package com.haimcohen.spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;

/**
 * Created by haimcohen on 31/08/2016.
 */
public class SimpleSparkTest {

    public static void main(String[] args) {
        try {
            FileUtils.deleteDirectory(new File("data/cloud/output"));

            SparkSession spark = SparkSession.builder()
                    .appName("cloud-init-parser")
                    .master("local[*]")
                    .getOrCreate();

            JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            JavaRDD<String> lines = sc.textFile("data/cloud/cloud-init.log"); //Whole line

            int comId = 3;
            Broadcast<Integer> bComId = sc.broadcast(comId);

            JavaRDD<String> components = lines.map(line -> {
                String[] splitted = line.split(" ");
                return splitted[bComId.getValue()];
            }); //:util.py[DEBUG]

            JavaPairRDD<String, Integer> pairs = components
                    .mapToPair(component -> new Tuple2<String, Integer>(component, 1));
            //(util.py[DEBUG]:, 1)

            JavaPairRDD<String, Integer> combined = pairs
                    .reduceByKey((a, b) -> a + b);
            //(util.py[DEBUG]:, 5)

            JavaPairRDD<Integer, String> reversed = combined
                    .mapToPair(tuple -> new Tuple2<Integer, String>(tuple._2, tuple._1));
            //(1546, util.py[DEBUG]:)

            JavaPairRDD<Integer, String> sorted = reversed.sortByKey(false);

//            sorted.collect().stream().forEach(System.out::println);

            sorted.saveAsTextFile("data/cloud/output");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
