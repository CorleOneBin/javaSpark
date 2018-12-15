package cn.zhubin.sparksql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class JSONDateSource {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JSONDateSource")
												.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		Dataset<Row> studentScoreDF = sqlContext.read().json("student.json");
		
		//针对学生成绩信息的DataFrame，注册临时表，查询分数大于80分学生的姓名
		studentScoreDF.registerTempTable("student_scores");
		Dataset<Row> goodStudnetDF = sqlContext.sql("select name,score from student_scores where score >= 80");
		//把这个DF转成list，并且取出其中的name属性
		List<String> goodStudentNames = goodStudnetDF.javaRDD().map(new Function<Row, String>() {

			public String call(Row row) throws Exception {
				return row.getAs("name");
			}
			
		}).collect();
		
		List<String> studentInfoJson = new ArrayList<>();
		studentInfoJson.add("{\"name\":\"Yasaka\",\"age\":18}");
		studentInfoJson.add("{\"name\":\"Xuruyun\",\"age\":17}");
		studentInfoJson.add("{\"name\":\"Liangyongqi\",\"age\":19}");
		JavaRDD<String> studentInfosRDD = sc.parallelize(studentInfoJson);
		Dataset<Row> studentInfoDF = sqlContext.read().json(studentInfosRDD);
		studentInfoDF.registerTempTable("student_infos");
		//取出大于80分学生的名字，年龄
		String sql = "select name,age from student_infos where name in (";
		for(int i = 0; i < goodStudentNames.size(); i++) {
			sql += "'"+goodStudentNames.get(i)+"'";
			if(i < goodStudentNames.size()-1) {
				sql+=",";
			}
		}
		sql+=")";
		System.out.println(sql);
		Dataset<Row> goodsStudentInfoDF =  sqlContext.sql(sql);
		
		//两张表join操作
		JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD = goodsStudentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			public Tuple2<String, Integer> call(Row row) throws Exception {
				
				return new Tuple2<String,Integer>(row.getAs("name"),Integer.valueOf(row.getAs("age")));
			}
			
		}).join(studentScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			public Tuple2<String, Integer> call(Row row) throws Exception {
				Long score = row.getAs("score");
				int scoreInt = score.intValue();
				return new Tuple2<String,Integer>(row.getAs("name"),scoreInt);
			}
			
		} ));
		
		
		//转换成ROWRDD
		JavaRDD<Row> goodStudentsRowRDD = goodStudentsRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

			public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
				return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
			}
		});
		
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
		
		StructType structType = DataTypes.createStructType(fields);
		Dataset<Row> goodStudentsDS = sqlContext.createDataFrame(goodStudentsRowRDD, structType);
		goodStudentsDS.write().format("json").mode(SaveMode.Overwrite).save("goodStudentJson");
	}
}
