package cn.zhubin.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class CartesianOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CartesianOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> clothes = Arrays.asList("ţ����","Ƥ����","����");
		List<String> clothes1 = Arrays.asList("ţ�п�","Ƥ���","�ڿ�");
		JavaRDD<String> rdd = sc.parallelize(clothes);
		JavaRDD<String> rdd2 = sc.parallelize(clothes1);
		rdd.cartesian(rdd2).foreach(new VoidFunction<Tuple2<String,String>>() {
			
			public void call(Tuple2<String, String> tuple) throws Exception {
				System.out.println(tuple._1 + "--" +tuple._2);
			}
		});
	}
}
