package cn.zhubin.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class HiveDataSource {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("HiveDataSource222");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//����Ҫ����SparkContext��������JavaSparkContext
		HiveContext hiveContext = new HiveContext(sc.sc());
		
		//�����ж��Ƿ�洢��student_infos���ű�����洢����ɾ��
		hiveContext.sql("DROP TABLE IF EXISTS student_infos");
		//�ؽ�
		hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");
		//��������
		hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/student_infos.txt' INTO student_infos");
		
		//һ���ķ�ʽ����������
		hiveContext.sql("DROP TABLE IF EXISTS student_scores");
		hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING,score INT)");
		hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/student_scores.txt' INTO student_scores");
		
		//�������ű���ѯ�ɼ�����80�ֵ�ѧ��
		Dataset<Row> goodstudentsDF = hiveContext.sql("SELECT si.name , si.age, ss.score FROM student_infos si JOIN student_scores  ss ON si.name=ss.name where ss.score >= 80");
		
		//�õ�������ݺ󣬻��ô��HIVE����
		hiveContext.sql("DROP TABLE IF EXISTS good_students_infos");
		hiveContext.sql("CREATE TABLE IF NOT EXISTS good_students_infos (name STRING, age INT,score INT)");
		goodstudentsDF.registerTempTable("temp");
		hiveContext.sql("insert into good_students_infos select name,age,score from temp");

		
		//��ô��ȡHIVE���е�����
		Dataset<Row> temp = hiveContext.table("good_students_infos");
		Row[] rows = (Row[]) temp.collect();
		for(Row row : rows) {
			System.out.println(row);
		}
		System.out.println("successed");
	}
}
