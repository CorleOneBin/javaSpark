package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SortedByKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SortedByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String,Integer>> sorceList = 	Arrays.asList(
				new Tuple2<String,Integer>("zhubin",111),
				new Tuple2<String,Integer>("zhubin",120),
				new Tuple2<String,Integer>("wangyujuan",150));
		 JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(sorceList);
		rdd.sortByKey(false).foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple._1);
			}
		});
		sc.close();
	}
}
