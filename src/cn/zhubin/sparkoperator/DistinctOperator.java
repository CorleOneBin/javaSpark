package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class DistinctOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DistinctOperator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> names = Arrays.asList("xuyun","zjubin","zhubin","zhubin");
		JavaRDD<String> rdd1 = sc.parallelize(names,2);
		rdd1.distinct().foreach(new VoidFunction<String>() {
			
			public void call(String str) throws Exception {
				System.out.println(str);
			}
		});
		sc.close();
	}
}
