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
		//这里要的是SparkContext，而不是JavaSparkContext
		HiveContext hiveContext = new HiveContext(sc.sc());
		
		//这里判断是否存储过student_infos这张表，如果存储过则删除
		hiveContext.sql("DROP TABLE IF EXISTS student_infos");
		//重建
		hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");
		//加载数据
		hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/student_infos.txt' INTO student_infos");
		
		//一样的方式导入其他表
		hiveContext.sql("DROP TABLE IF EXISTS student_scores");
		hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING,score INT)");
		hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/student_scores.txt' INTO student_scores");
		
		//关联两张表，查询成绩大于80分的学生
		Dataset<Row> goodstudentsDF = hiveContext.sql("SELECT si.name , si.age, ss.score FROM student_infos si JOIN student_scores  ss ON si.name=ss.name where ss.score >= 80");
		
		//得到这个数据后，还得存回HIVE表中
		hiveContext.sql("DROP TABLE IF EXISTS good_students_infos");
		hiveContext.sql("CREATE TABLE IF NOT EXISTS good_students_infos (name STRING, age INT,score INT)");
		goodstudentsDF.registerTempTable("temp");
		hiveContext.sql("insert into good_students_infos select name,age,score from temp");

		
		//怎么读取HIVE表中的数据
		Dataset<Row> temp = hiveContext.table("good_students_infos");
		Row[] rows = (Row[]) temp.collect();
		for(Row row : rows) {
			System.out.println(row);
		}
		System.out.println("successed");
	}
}
