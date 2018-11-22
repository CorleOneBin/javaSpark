package cn.zhubin.sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SecoundKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SecoundKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd = sc.textFile("sort.txt");
		JavaPairRDD<SecondSortKey, String> pairRDD = rdd.mapToPair(new PairFunction<String, SecondSortKey , String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<SecondSortKey, String> call(String line) throws Exception {
				String[] arrs = line.split(" ");
				SecondSortKey sck = new SecondSortKey(
										Integer.valueOf(arrs[0]), 
										Integer.valueOf(arrs[2]));
				return new Tuple2<SecondSortKey, String>(sck,line);
			}
		});
		JavaRDD<String> results = pairRDD.sortByKey(false).map(new Function<Tuple2<SecondSortKey,String>, String>() {

			public String call(Tuple2<SecondSortKey, String> tuple) throws Exception {
				return tuple._2;
			}
		});
		results.foreach(new VoidFunction<String>() {
			
			public void call(String result) throws Exception {
				System.out.println(result);
			}
		});
		sc.close();
		
	}
}
