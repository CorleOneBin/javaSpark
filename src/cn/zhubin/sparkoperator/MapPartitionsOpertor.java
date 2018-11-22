package cn.zhubin.sparkoperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

public class MapPartitionsOpertor {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MapPartitionsOpertor").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> names = Arrays.asList("xurunyun","zhubin","wangyujuan");
		JavaRDD<String> namesRDD = sc.parallelize(names);
		
		final Map<String,Integer> sorceMap = new HashMap<String,Integer>();
		sorceMap.put("xurunyun", 100);
		sorceMap.put("zhubin", 150);
		sorceMap.put("wangyujuan", 200);
		
		//mapPartitions
		//map���ӣ�һ�ξʹ���һ��partitions��һ������
		//mapPartitions���ӣ�һ�δ���һ��partitions�����е�����
		
		//�Ƽ���ʹ�ó���
		//������RDD�����ݲ����ر�࣬��ô����MapPartitons���Ӵ���map���ӣ����Լӿ촦���ٶ�
		//����100�������ݣ�һ��partition�������10�������ݣ�������ʹ��
		//�ڴ������partions̫��
		
		JavaRDD<Integer> sorceRDD = namesRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {

			
			private static final long serialVersionUID = 1L;

			public Iterator<Integer> call(Iterator<String> iterator) throws Exception {
				List<Integer> list = new ArrayList<Integer>();
				while(iterator.hasNext()) {
					String name = iterator.next();
					Integer sorce = sorceMap.get(name);
					list.add(sorce);
				}
				return list.iterator();
			}
			
		});
		sorceRDD.foreach(new VoidFunction<Integer>() {
			
			public void call(Integer result) throws Exception {
				System.out.println(result);
			}
		});
		
		sc.close();
	}
}
