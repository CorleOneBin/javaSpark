package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SaveAsTextFile {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SaveAsTextFile").
				setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numberList = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD =  sc.parallelize(numberList);
		 JavaRDD<Integer> resultRDD = numberRDD.map(new Function<Integer, Integer>() {

			public Integer call(Integer result) throws Exception {
				return result*2;
			}
			
		});
		resultRDD.saveAsTextFile("hdfs://node1:9000/save_dir");
		sc.close();
	}
}
