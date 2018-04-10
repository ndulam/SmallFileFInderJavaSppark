package com.naresh.org.SparkJavaApp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.io.IOException;

import org.apache.hadoop.fs.*;

public class SampleSpark {

	public static void main(String[] args) throws IOException
	{
	
	
		
		 // Create a Java Spark Context.
	    SparkConf conf = new SparkConf().setAppName("wordCount");
	    JavaSparkContext sc = new JavaSparkContext(conf);
		FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	    // Load our input data.
		Long dirSize = fs.getContentSummary(new Path("hdfs://quickstart.cloudera:8020/tx/news/")).getLength();
		float fileNum = dirSize/(512 * 1024 * 1024) ;
		System.out.print("File Number os "+ fileNum);
		
	}

}
