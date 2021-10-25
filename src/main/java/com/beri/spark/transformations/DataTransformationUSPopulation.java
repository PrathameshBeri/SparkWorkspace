package com.beri.spark.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.split;

public class DataTransformationUSPopulation {
    public static void main(String args[]) {

        DataTransformationUSPopulation app = new DataTransformationUSPopulation();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder().master("local").appName("data transformation").getOrCreate();
        Dataset<Row> intermediateDf = spark.read().format("csv").option("header", true).option("inferSchema", true).
                load("src/main/resources" +
                        "/PEP_2017_PEPANNRES.csv");

        intermediateDf = intermediateDf
                .drop("GEO.id")
                .withColumnRenamed("GEO.id2", "id")
                .withColumnRenamed("GEO.display-label", "label")
                .withColumnRenamed("rescen42010", "real2010")
                .drop("resbase42010")
                .withColumnRenamed("respop72010", "est2010")
                .withColumnRenamed("respop72011", "est2011")
                .withColumnRenamed("respop72012", "est2012")
                .withColumnRenamed("respop72013", "est2013")
                .withColumnRenamed("respop72014", "est2014")
                .withColumnRenamed("respop72015", "est2015")
                .withColumnRenamed("respop72016", "est2016")
                .withColumnRenamed("respop72017", "est2017");

        intermediateDf = intermediateDf.withColumn("countyState", split(intermediateDf.col("label"), ","))
                .withColumn("stateId", expr("int(id/1000)"))
                .withColumn("countyId", expr("int(id%1000)"));
        intermediateDf.printSchema();

        intermediateDf = intermediateDf
                .withColumn("state", intermediateDf.col("countyState").getItem(1))
                .withColumn("county", intermediateDf.col("countyState").getItem(0))
                .drop("countyState");

        intermediateDf.printSchema();

        intermediateDf.sample(0.1).show(10, false);


        Dataset<Row> statDf = intermediateDf
                .withColumn("diff",expr("real2010 - est2010"))
                .withColumn("growth", expr("est2017 - est2011"))
                .drop("id")
                .drop("label")
                .drop("real2010")
                .drop("est2010")
                .drop("est2011")
                .drop("est2012")
                .drop("est2013")
                .drop("est2014")
                .drop("est2015")
                .drop("est2016")
                .drop("est2017");
        statDf.printSchema();
        statDf.sample(0.1).show(5, false);
    }
}
