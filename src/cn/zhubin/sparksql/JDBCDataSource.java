package cn.zhubin.sparksql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class JDBCDataSource {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		Map<String,String> options = new HashMap<>();
		options.put("url", "jdbc:mysql://node1:3306/student");
		options.put("dbtable", "student_info");
		options.put("user", "root");
		options.put("password", "zhubin");
		/*name and  id*/
		Dataset<Row>  studentInfoDF = sqlContext.read().format("jdbc").options(options).load();
		
		/*name and score*/
		options.put("dbtable", "student_scores");
		Dataset<Row>  studentScoreDF = sqlContext.read().format("jdbc").options(options).load();
		
		//将两个DataFrame转换成JavaParirRDD，进行join操作
		JavaPairRDD<String, Tuple2<Integer, Integer>> studentsRDD = studentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String,Integer>(row.getString(0),
						Integer.valueOf(String.valueOf(row.get(1))));
			}
		}).join(studentScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String,Integer>(row.getString(0),
						Integer.valueOf(String.valueOf(row.get(1))));
			}
		}));
		
		//过滤
		JavaRDD<Row> studentsRowRDD = studentsRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

			public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
				return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
			}
		}).filter(new Function<Row, Boolean>() {
			
			public Boolean call(Row row) throws Exception {
				if(row.getInt(2)>80) {
					return true;
				}
				return false;
			}
		});
		
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("sorce", DataTypes.IntegerType, true));
		StructType schma = DataTypes.createStructType(structFields);
		
		Dataset<Row> studentDF = sqlContext.createDataFrame(studentsRowRDD, schma);
		List<Row> list = studentDF.javaRDD().collect();
		for(Row row : list) {
			System.out.println(row);
		}
		
		//将DataFrame数据保存到mysql表中
		//这种方式在公司里面会很常用！ 有可能插入mysql，也有可能插入Redis或者Hbase都是有可能的
		
		studentDF.javaRDD().foreach(new VoidFunction<Row>() {
			
			public void call(Row row) throws Exception {
				String sql = "insert into good_student_infos values("
						+"'"+row.getString(0)+"',"
						+Integer.valueOf(String.valueOf(row.get(1)))+","
						+Integer.valueOf(String.valueOf(row.get(2)))+")";
				System.out.println(sql);
				Class.forName("com.mysql.jdbc.Driver");
				Connection conn = null;
				Statement stat = null;
				
				conn=DriverManager.getConnection("jdbc:mysql://node1:3306/student","root","zhubin");
				stat = conn.createStatement();
				stat.executeUpdate(sql);
				stat.close();
				conn.close();
			}
		});
	}
}
