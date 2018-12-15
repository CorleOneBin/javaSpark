package cn.zhubin.sparkoperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class GroupBykeyOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("GroupBykeyOperator")
				.setMaster("local[1]");
		conf.set("spark.default.parallelism", "1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String,Integer>> sorceList = Arrays.asList(
				new Tuple2<String, Integer>("zhubin",80),
				new Tuple2<String, Integer>("wangyujuan",100),
				new Tuple2<String, Integer>("zhubin",120),
				new Tuple2<String, Integer>("xiaoming",120),
				new Tuple2<String, Integer>("hanmeimei",120),
				new Tuple2<String, Integer>("hanmei",120),
				new Tuple2<String, Integer>("hanimei",120),
				new Tuple2<String, Integer>("haeimei",120));
		
		JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(sorceList);
		JavaPairRDD<String,Integer>  mapRDD = rdd.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				return new Tuple2<String, Integer>(tuple._1, tuple._2+2);
			}
		});
		JavaPairRDD<String, Integer> results = mapRDD.repartition(3);
		JavaPairRDD<String, Iterable<Integer>> finalResults = results.groupByKey();
		 JavaRDD<String> results2 = finalResults.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String,Iterable<Integer>>>, Iterator<String>>() {
			
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(Integer index, Iterator<Tuple2<String, Iterable<Integer>>> iterator)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iterator.hasNext()) {
					Tuple2 temp = iterator.next();
					list.add(index + ":" + temp._1 +"--"+temp._2);
				}
				return list.iterator();
			}
		}, true);
		 results2.foreach(new VoidFunction<String>() {
			public void call(String result) throws Exception {
				System.out.println(result);
			}
		});
		sc.close();
	}
}
