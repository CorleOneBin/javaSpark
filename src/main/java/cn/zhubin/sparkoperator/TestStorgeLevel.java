package cn.zhubin.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestStorgeLevel {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("TestStorgeLevel");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> text = sc.textFile("a.txt");
		text = text.cache();
		long starttime = System.currentTimeMillis();
		long count = text.count();	
		long endtime = System.currentTimeMillis();
		long time = endtime - starttime;
		System.out.println(time);
		
		long starttime2 = System.currentTimeMillis();
		long count2 = text.count();	
		long endtime2 = System.currentTimeMillis();
		long time2 = endtime2 - starttime2;
		System.out.println(time2);
		
		
	}
}
