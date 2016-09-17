package com.haimcohen.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.UUID;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;

/**
 * Example of how to use User Defined Function to add column with UUID to the data set.
 * This example use the following Spark capabilities:
 * <li>Load CSV file</li>
 * <li>Create and use User Defined Function in Spark SQL</li>
 * <li>Adding column to existing data set</li>
 * <li>Saving data set back to new CSV</li>
 *
 * Created by Haim Cohen on 16/09/2016.
 */
public class SparkSqlAddUUIDColumn {

    public static void main(String[] args) {
        try {
            //Create session in local mode
            SparkSession spark = SparkSession.builder()
                    .appName("uuid-column")
                    .master("local[*]")
                    .getOrCreate();

            //Load existing CSV file from disk
            Dataset<Row> bankDf = spark
                    .read()
                    .option("header", true)
                    .option("delimiter", ";")
                    .csv("data/bank/bank.csv");

            //Register user defined function that creates UUID
            spark.udf().register("createUuid",
                    // The String parmeter below is not in use. I could not find
                    // a way to define UDF with zero params. Also using a UDF must be with
                    // at least one column...
                    (String s) -> UUID.randomUUID().toString()
                    , DataTypes.StringType);

            //Use the registerd UDF
            bankDf.withColumn("UUID", callUDF("createUuid", lit("a"))) //Lit is used as hack to pass parameter not in use
                    .write()
                    .mode("overwrite")
                    .option("header", true)
                    .csv("output/bank/uuid-output");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
