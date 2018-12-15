package cn.zhubin.sparkoperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

public class MapPartitionsWithIndexOpertor {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MapPartitionsWithIndexOpertor")
				.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> names = Arrays.asList("zhubin","wangyujuan","hanmeimei");
		JavaRDD<String> nameRDD = sc.parallelize(names,2);
		
		/**
		 * �������Ĳ����ֱ�Ϊ partitions�ı�ţ�������
		 * ͨ��MapPartitionsWithIndex������ӣ������õ�ÿ��partition��index
		 */
		JavaRDD<String> result = nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				List<String> list = new ArrayList<String>();
				while(iterator.hasNext()) {
					String result = index + ":" + iterator.next();
					list.add(result);
				}
				return list.iterator();
			}
			
		}, true);
		
		result.foreach(new VoidFunction<String>() {
			

			private static final long serialVersionUID = 1L;

			public void call(String result) throws Exception {
				System.out.println(result);
			}
		});
		
		sc.close();
	}
}
