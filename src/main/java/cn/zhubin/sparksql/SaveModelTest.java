package cn.zhubin.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class SaveModelTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SaveModelTest").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
	    SQLContext sqlContext = new SQLContext(sc);
		
		Dataset<Row> ds = sqlContext.read().format("json").load("people.json");
		ds.write().mode(SaveMode.Overwrite).save("good");
		ds.write().mode(SaveMode.Append).save("good");
		ds.write().mode(SaveMode.Ignore).save("good");
	}
}
