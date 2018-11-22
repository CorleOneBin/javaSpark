package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class MapOpertor {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MapOpertor").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		
		JavaRDD<Integer> results = numberRDD.map(new Function<Integer, Integer>() {

			public Integer call(Integer i) throws Exception {
				return i*10;
			}
			
		});
		 
		results.foreach(new VoidFunction<Integer>() {
			
			public void call(Integer result) throws Exception {
				System.out.println(result);
			}
		});
		
		sc.close();
	}
}
