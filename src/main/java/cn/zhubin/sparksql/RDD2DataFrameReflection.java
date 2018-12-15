package cn.zhubin.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class RDD2DataFrameReflection {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<String> strRDD = sc.textFile("student.txt");
		JavaRDD<Student> studentRDD = strRDD.map(new Function<String, Student>() {

			public Student call(String str) throws Exception {
				String[] arrs = str.split(",");
				Student stu = new Student();
				stu.setId(Integer.valueOf(arrs[0]));
				stu.setName(arrs[1]);
				stu.setAge(Integer.valueOf(arrs[2]));
				return stu;
			}
			
		});
		
		//使用反射的方式转换为DataFrame
		Dataset<Row> studentDS = sqlContext.createDataFrame(studentRDD, Student.class);
		studentDS.printSchema();
		//注册一个零时表
		studentDS.registerTempTable("student");
		
		Dataset<Row> teenagerDS = sqlContext.sql("select * from student where age <= 18");
		//转换为RDD
		JavaRDD<Row> teenagerRDD = teenagerDS.toJavaRDD();
		JavaRDD<Student> teenamgerStudentRDD = teenagerRDD.map(new Function<Row, Student>() {

			public Student call(Row row) throws Exception {
				int id = row.getAs("id");
				int age = row.getAs("age");
				String name = row.getAs("name");
				Student stu = new Student();
				stu.setId(id);
				stu.setAge(age);
				stu.setName(name);
				
				return stu;
			}
			
		});
		teenamgerStudentRDD.foreach(new VoidFunction<Student>() {
			
			public void call(Student stu) throws Exception {
				System.out.println(stu);
			}
		});
		sc.close();
	}
}
