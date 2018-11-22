package cn.zhubin.sparkoperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class RepartitionOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CoalseceOpertor")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//repartition ���ӣ����������⽫RDD��partition������������߼���
		//һ�������ʹ�ó�����ʹ��Spark SQL��HIVE�в�ѯ����ʱ��Spark SQLh�����HIVE
		//��Ӧ��hdfs�ļ���block�������������س�����RDD��partition�ж��ٸ�
		//����Ĭ�ϵ�partitiond�������������޷����õ�
		//��Щʱ�򣬿��������Զ����õ�partition�������������ˣ�Ϊ�˽����Ż�
		//������߲��жȣ����Ƕ�RDDʹ��repartitionsu����
		
		List<String> staffList = Arrays.asList("zhubin1","zhubin2","zhubin3","zhubin4",
			"zhubin5","zhubin6","zhubin7","zhubin8","zhubin9","zhubin10","zhubin11","zhubin12","zhubin13"
			,"zhubin14","zhubin15");
		JavaRDD<String> staffRDD = sc.parallelize(staffList,3);
		JavaRDD<String> staffRDD2 = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

	
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(Integer index, Iterator<String> staff) throws Exception {
				List<String> list = new ArrayList<String>();
				while(staff.hasNext()) {
					String result = "����"+"[" +(index+1)+"]:"+staff.next();
					list.add(result);
				}
				return list.iterator();
			}
		}, true);
		for(String staffInfo : staffRDD2.collect()) {	
			System.out.println(staffInfo);
		}
		JavaRDD<String> staffRDD3 = staffRDD2.repartition(6);
		JavaRDD<String> staffRDD4 = staffRDD3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				List<String> list = new ArrayList<>();
				while(iterator.hasNext()) {
					list.add("����"+"["+(index+1)+"]"+iterator.next());
				}
				return list.iterator();
			}
		}, true);
		for(String staffInfo : staffRDD4.collect()) {
			System.out.println(staffInfo);
		}
		sc.close();
	}
}
