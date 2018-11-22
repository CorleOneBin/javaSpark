package cn.zhubin.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class LoadSave {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LoadSave").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		//ͳһ��Դ������
		Dataset<Row> ds = sqlContext.read().load("a.parquet");
		
		ds.printSchema();
		ds.show();
		//ͳһ��Դ����,���浽�ò������ֵ��ļ�����
		ds.select("name","color").write().save("peopleName.parquet");
	}
}
