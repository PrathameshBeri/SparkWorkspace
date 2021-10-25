package com.beri.spark.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class IngestFromCSVAndCreateFauxView {
    public static void main(String args[]){

        IngestFromCSVAndCreateFauxView ss = new IngestFromCSVAndCreateFauxView();

        ss.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder().appName("csv create view").master("local").getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo", DataTypes.StringType,true),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)

        });

        Dataset<Row> df = spark.read().format("csv").option("header", true).schema(schema).load("src/main/resources" +
                        "/populationbycountry19802010millions.csv");

        df.createOrReplaceTempView("geocities");

        Dataset<Row> smallcountries = spark.sql("select * from geocities where yr1980 < 1 Order by 2 LIMIT 50");

        smallcountries.show(10, false);

    }
}
