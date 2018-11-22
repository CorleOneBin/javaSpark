package cn.zhubin.sparksql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RDD2DataFrameDynamic {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("RDD2DataFrameDynamic")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> lines = sc.textFile("student.txt");
		JavaRDD<Row> rows = lines.map(new Function<String, Row>() {

			public Row call(String line) throws Exception {
				String[] strs = line.split(",");
				return RowFactory.create(Integer.valueOf(strs[0]),strs[1],Integer.valueOf(strs[2]));
			}
			
		});
		
		//动态构造元数据，还有一种方式j就是通过反射的方式来构建出DataFrame，这里是用动态的来构建
		//有些时候我们一开始不确定有哪些列，而这些列需要通过数据库比如mysql或者配置文件加载出来
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		
		StructType schema = DataTypes.createStructType(fields);
		
		Dataset<Row> studentDS = sqlContext.createDataFrame(rows, schema);
		studentDS.registerTempTable("stu");
		
		Dataset<Row> teenagerDS = sqlContext.sql("select * from stu where age <= 18");
		List<Row> list = teenagerDS.javaRDD().collect();
		for(Row row : list) {
			System.out.println(row);
		}
		sc.close();
	}
}
