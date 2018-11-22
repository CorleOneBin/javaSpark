package cn.zhubin.sparktest;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

public class BroadCastValue {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("BroadCastValue")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		final int f = 3;
		final Broadcast<Integer> broadCastFactor = sc.broadcast(f);
		List<Integer> list = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		JavaRDD<Integer> resultRDD = listRDD.map(new Function<Integer, Integer>() {

			public Integer call(Integer num) throws Exception {
//				return num * f;
				return num * broadCastFactor.value();
			}
		});
		resultRDD.foreach(new VoidFunction<Integer>() {
			
			public void call(Integer num) throws Exception {
				System.out.println(num);
			}
		});
		sc.close();
	}
}
