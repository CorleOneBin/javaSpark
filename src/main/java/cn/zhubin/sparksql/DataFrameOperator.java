package cn.zhubin.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataFrameOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DataFrameOperator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		//�����ݿ��������ȫ�������Ϊһ�ű�
		Dataset<Row> df = sqlContext.read().json("student.json");
		
		//��ӡԪ��
		df.show();
		
		//��ӡԪ����
		df.printSchema();
		
		//��ѯ������
		df.select("name").show();
		df.select(df.col("name"),df.col("score").plus(1)).show();
		
		//����
		df.filter(df.col("score").gt(80)).show();
		
		//����ĳһ�з���Ȼ��ͳ��count
		df.groupBy("score").count().show();
		
		sc.close();
	}
}
