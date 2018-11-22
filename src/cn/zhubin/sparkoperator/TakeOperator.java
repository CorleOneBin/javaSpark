package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TakeOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SampleOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> names = Arrays.asList("zhubin","wangyujuan","Jack","linda","hanmeimei","xiaoming");
		JavaRDD<String> rdd = sc.parallelize(names);
		List<String> list = rdd.take(3);
		for(String str : list) {
			System.out.println(str);
		}
		sc.close();
	}
}
