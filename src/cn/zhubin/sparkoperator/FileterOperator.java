package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class FileterOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("FileterOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> number = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(number);
		
		JavaRDD<Integer> result = numberRDD.filter(new Function<Integer, Boolean>() {
			
			public Boolean call(Integer number) throws Exception {
				return number % 2 == 0;
			}
		});
		
		result.foreach(new VoidFunction<Integer>() {
			
			public void call(Integer result) throws Exception {
				System.out.println(result);
			}
		});
		
		sc.close();
	}
}
