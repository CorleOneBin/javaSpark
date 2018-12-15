package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

public class FlatMapOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("FlatMapOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> lineList = Arrays.asList("hello zhubin","hello wangyujuan");
		JavaRDD<String> lines = sc.parallelize(lineList);
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {


			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String lines) throws Exception {
				return Arrays.asList(lines.split(" ")).iterator();
			}
		});
		words.foreach(new VoidFunction<String>() {
			
			public void call(String word) throws Exception {
				System.out.println(word);
			}
		});
		sc.close();
	}
}
