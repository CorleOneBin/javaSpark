package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class ReduceByKeyOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ReduceByKeyOperator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String,Integer>> sorceList = Arrays.asList(
				new Tuple2<String,Integer>("zhubin",111),
				new Tuple2<String,Integer>("zhubin",120),
				new Tuple2<String,Integer>("wangyujuan",150));
		 JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(sorceList);
		 rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer va1, Integer va2) throws Exception {
				return va1+va2;
			}
		}).foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple._1 + ":" + tuple._2);
			}
		});
		sc.close();
	}
}
