package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class UnionOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("UnionOperator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> names = Arrays.asList("xuyun","zjubin");
		List<String> names1 = Arrays.asList("zhubin","wangyujuan");
		JavaRDD<String> rdd1 = sc.parallelize(names,2);
		JavaRDD<String> rdd2 =sc.parallelize(names1,2);
		rdd1.union(rdd2).foreach(new VoidFunction<String>() {
			
			public void call(String str) throws Exception {
				System.out.println(str);
			}
		});
		sc.close();
	}
}
