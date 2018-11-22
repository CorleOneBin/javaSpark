package cn.zhubin.sparktest;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class AccumulatorValue {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		final Accumulator<Integer> sum = sc.accumulator(0,"Our accumulator");
		List<Integer> list = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		listRDD.foreach(new VoidFunction<Integer>() {
			
			public void call(Integer num) throws Exception {
				sum.add(num);
			}
		});
		System.out.println(sum.value());
		try {
			Thread.sleep(1000*1000*60);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sc.close();
	}
}
