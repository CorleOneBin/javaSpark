package cn.zhubin.sparktest;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TopN {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TopN").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> line = sc.textFile("test.txt");
		JavaPairRDD<Integer, String> pairs = line.mapToPair(new PairFunction<String, Integer, String>() {

			public Tuple2<Integer, String> call(String line) throws Exception {
				return new Tuple2<Integer,String>(Integer.valueOf(line),line);
			}
		});
		List<String> list = pairs.sortByKey(false).map(new Function<Tuple2<Integer,String>, String>() {

			public String call(Tuple2<Integer, String> tuple) throws Exception {
				return tuple._2;
			}
			
		}).take(3);
		for(String str : list) {
			System.out.println(str);
		}
		sc.close();
		
	}
}
