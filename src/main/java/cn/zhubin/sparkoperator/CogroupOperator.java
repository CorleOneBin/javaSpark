package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class CogroupOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CogroupOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer,String>> nameList = Arrays.asList(new Tuple2<Integer,String>(1,"zhubin"),
															  new Tuple2<Integer,String>(2,"wangyujuan"),
															  new Tuple2<Integer,String>(2,"zhubin"),
															  new Tuple2<Integer,String>(3,"hanmeimei"));
		List<Tuple2<Integer,Integer>> sorceList = Arrays.asList(new Tuple2<Integer,Integer>(1,40),
				  new Tuple2<Integer,Integer>(2,40),
				  new Tuple2<Integer,Integer>(2,30),
				  new Tuple2<Integer,Integer>(3,20),
				  new Tuple2<Integer,Integer>(3,100));
		JavaPairRDD<Integer, String>  rdd1 = sc.parallelizePairs(nameList);
		JavaPairRDD<Integer, Integer>  rdd2 = sc.parallelizePairs(sorceList);
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> results = rdd1.cogroup(rdd2);
		results.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {
			
			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> tuple) throws Exception {
				System.out.println("id:"+tuple._1);
				System.out.println("name:"+tuple._2._1);
				System.out.println("sorce:"+tuple._2._1);
			}
		});
		sc.close();
	}
}
