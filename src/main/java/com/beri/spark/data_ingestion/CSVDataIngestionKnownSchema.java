package com.beri.spark.data_ingestion;

import org.apache.hadoop.fs.DF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SessionStateBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CSVDataIngestionKnownSchema {
	public static void main(String args[]) {
		
		CSVDataIngestionKnownSchema cv = new CSVDataIngestionKnownSchema();
		cv.start();
		
	}

	private void start() {
			
		SparkSession sess = SparkSession.builder()
							.appName("csv ingestion known schema")
							.master("local")
							.getOrCreate();
		StructType schema = DataTypes.createStructType(new StructField[] {
				DataTypes.createStructField("id", DataTypes.IntegerType, false),
				DataTypes.createStructField("authorId", DataTypes.IntegerType, false),
				DataTypes.createStructField("title", DataTypes.StringType, false),
				DataTypes.createStructField("releaseDate", DataTypes.DateType, false),
				DataTypes.createStructField("link", DataTypes.StringType, false)
		});
		
		Dataset<Row> df = sess.read().format("csv")
							.option("header", true)
							.option("sep", ";")
							.option("quote", "*")
							.option("mulitline", true)
							.option("dateFormat", "MM/dd/yyyy")
							.schema(schema)
							.load("src/main/resources/books.csv");
		df.printSchema();
		df.show(5,15);
		
		
	}
}
