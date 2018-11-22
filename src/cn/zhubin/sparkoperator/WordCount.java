package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("wc");
		JavaSparkContext sc = new JavaSparkContext(conf);
		

		JavaRDD<String> text = sc.textFile("a.txt");
		JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;
			
			@SuppressWarnings("unchecked")
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
			
		});
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

	
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				
				return new Tuple2<String,Integer>(word,1);
			}
			
		});
		/**
		 * �ѽ�����
		 */
		JavaPairRDD<String, Integer> results = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
		
			private static final long serialVersionUID = 1L;

			public Integer call(Integer value1, Integer value2) throws Exception {
				return value1+value2;
			}
		});
		/**
		 * ��key��value������
		 */
		JavaPairRDD<Integer, String> temp = results.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

		
			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				return new Tuple2<Integer, String>(tuple._2 , tuple._1);
			}
			
		});
		/**
		 * ����key����Ȼ���ٰ�key��value������
		 */
		JavaPairRDD<String,Integer> sorted = temp.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

			
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
				return new Tuple2<String, Integer>(tuple._2 , tuple._1);
			}
		});
		/**
		 * ���
		 */
		sorted.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println("words:"+tuple._1 + "   num:" + tuple._2);
			}
		});
		
		sc.close();
	}
}
