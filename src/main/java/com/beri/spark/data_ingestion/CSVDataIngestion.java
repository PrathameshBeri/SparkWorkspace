package com.beri.spark.data_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVDataIngestion {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			CSVDataIngestion cs = new CSVDataIngestion();
			cs.run();
	}
	
	public void run() {
		
		SparkSession spark = SparkSession.builder()
							.appName("CSV Ingestion")
							.master("local")
							.getOrCreate();
		Dataset<Row> df = spark.read().format("csv")
				.option("header", true)
				.option("sep", ";")
				.option("multiline", true)
				.option("quote", "*")
				.option("dateFormat", "M/d/y")
				.option("inferSchema", true)
				.load("src/main/resources/books.csv");
		System.out.println("Excerpt of the DataFrame content: ");
		df.show(7,90);
		System.out.println("DataFrame's schema");
		df.printSchema();
		
	}

}
