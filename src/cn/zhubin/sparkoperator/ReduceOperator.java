package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ReduceOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ReduceOperator").
				setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numberList = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD =  sc.parallelize(numberList);
		int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer va1, Integer va2) throws Exception {
				return va1+va2;
			}
		});
		System.out.println(sum);
		sc.close();
	}
}
