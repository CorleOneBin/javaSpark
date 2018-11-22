package cn.zhubin.sparksql;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCDataSource3 {
    public static void main(String args[]){
        SparkConf conf = new SparkConf().setAppName("JDBCDataSource3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String,String> map = new HashMap<>();
        map.put("url","jdbc:mysql://node1:3306/student");
        map.put("user","root");
        map.put("password","zhubin");
        map.put("dbtable","student_info");

        Dataset<Row> studentInfosDf =  sqlContext.read().format("jdbc").options(map).load();

        map.put("dbtable","student_score");
        Dataset<Row> studentScoresDF =  sqlContext.read().format("jdbc").options(map).load();

        JavaPairRDD<String,Tuple2<Integer,Integer>> studentRDD =  studentInfosDf.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String,Integer>(row.getString(0),row.getInt(1));
            }

        }).join(studentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String,Integer>(row.getString(0),row.getInt(1));
            }

        }));

        JavaRDD<Row> studentRow =  studentRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer> >, Row>() {

            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
            }
        });

        JavaRDD<Row> goodStudentRow =  studentRow.filter(new Function<Row, Boolean>() {

            public Boolean call(Row row) throws Exception {
                if(row.getInt(2) > 80){
                    return true;
                }
                return false;
            }
        });

        List<StructField> structFieldList = new ArrayList<>();
        structFieldList.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFieldList.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFieldList.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType structType =  DataTypes.createStructType(structFieldList);

        Dataset<Row> goodStudentDF =  sqlContext.createDataFrame(goodStudentRow, structType);
        List<Row> list = goodStudentDF.javaRDD().collect();
        for(Row row : list){
            System.out.println(row);
        }

        goodStudentDF.javaRDD().foreach(new VoidFunction<Row>() {

            public void call(Row row) throws Exception {
                String sql = "insert into student values ("
                        +"'"+ row.getString(0) +"'," +
                        row.getInt(1)+","+row.getInt(2)+")";
                System.out.println("sql");
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = null;
                Statement statement = null;

                conn = DriverManager.getConnection("jdbc:mysql://node1:3306/student","root","zhubin");
                statement =  conn.createStatement();

                statement.execute(sql);

                statement.close();
                conn.close();

            }

        });



    }
}
