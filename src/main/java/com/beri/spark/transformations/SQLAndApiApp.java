package com.beri.spark.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SQLAndApiApp {
    public static void main(String args[]){

        SQLAndApiApp abc = new SQLAndApiApp();
        abc.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder().appName("sql and api").master("local").getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{

                DataTypes.createStructField("geo", DataTypes.StringType,true),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1981", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1982", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1983", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1984", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1985", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1986", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1987", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1988", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1989", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1990", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2010", DataTypes.DoubleType, true)
        });

        Dataset<Row> df = spark.read().format("csv").option("header",true).schema(schema).load("src/main/resources" +
                "/populationbycountry19802010millions.csv");

        //We drop extra columns which are not needed, real world scenario
        for(int i = 1981; i < 1991; i++){
            df = df.drop(df.col("yr" + i));
        }

        //we now add a new column which will hold the difference in the 1980 pop and the 2010 pop

        df = df.withColumn("growth", functions.expr("round((yr2010 - yr1980) * 1000000)"));
        df.createOrReplaceTempView("geodata");

        Dataset<Row> poplnGrowthNeg = spark.sql("select * from geodata " +
                                " where geo IS NOT NULL and growth <=0 "
                + "order by growth "
                + "limit 35;" );

        poplnGrowthNeg.show(15, false);

        Dataset<Row> poplnGrowthPos = spark.sql("select * from geodata " +
                "where geo IS NOT NULL and growth > 999999 "
                + "order by growth "
                + "limit 35;" );
        poplnGrowthPos.show(15, false);
    }
}
