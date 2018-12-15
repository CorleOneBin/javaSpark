package cn.zhubin.sparktest;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class GroupTopN {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("GroupTopN").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> line = sc.textFile("sorce.txt");
		JavaPairRDD<String, Integer> pairs = line.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String line) throws Exception {
				String[] arr = line.split(" ");
				return new Tuple2<String,Integer>(arr[0],Integer.valueOf(arr[1]));
			}
		});
		JavaPairRDD<String, Iterable<Integer>> groupPairs = pairs.groupByKey();
		JavaPairRDD<String, Iterator<Integer>> result = groupPairs.mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterator<Integer>>() {

			public Tuple2<String, Iterator<Integer>> call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
				
				List<Integer> list = new ArrayList<>();
				Iterable<Integer> iterable = tuple._2;
				Iterator<Integer> it = iterable.iterator();
				while(it.hasNext()) {
					list.add(it.next());
				}
				list.sort(new Comparator<Integer>() {

					public int compare(Integer o1, Integer o2) {
						return -(o1-o2);
					}
					
				});
				List<Integer> resultList = list.subList(0, 2);
				return new Tuple2<String,Iterator<Integer>>(tuple._1,resultList.iterator());
			}
			
		});
		result.foreach(new VoidFunction<Tuple2<String,Iterator<Integer>>>() {
			
			public void call(Tuple2<String, Iterator<Integer>> tuple) throws Exception {
				while(tuple._2.hasNext()) {
					System.out.println(tuple._1+":"+tuple._2.next());
				}
			}
		});
		sc.close();
	}
}
