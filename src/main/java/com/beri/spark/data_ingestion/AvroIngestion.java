package com.beri.spark.data_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroIngestion {
    public static void main(String args[]){

        AvroIngestion av = new AvroIngestion();
        av.start();

    }

    private void start() {

        SparkSession spark = SparkSession.builder().appName("avro ingestion")
                    .master("local").
                    getOrCreate();

        Dataset<Row> df = spark.read().format("avro").load("src/main/resources/weather.avro");
        df.printSchema();
        df.show(5,15);

    }

}
