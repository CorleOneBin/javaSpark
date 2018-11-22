package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class SampleOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SampleOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> names = Arrays.asList("zhubin","wangyujuan","Jack","linda","hanmeimei","xiaoming");
		JavaRDD<String> rdd = sc.parallelize(names,3);
		rdd.sample(false, 0.3).foreach(new VoidFunction<String>() {
			
			public void call(String val) throws Exception {
				System.out.println(val);
			}
		});
		sc.close();
	}
}
