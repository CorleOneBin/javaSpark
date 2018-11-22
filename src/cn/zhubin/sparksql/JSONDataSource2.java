package cn.zhubin.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JSONDataSource2 {
    public static void main(String args []){
        SparkConf conf = new SparkConf().setAppName("JSONDataSource2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //查出大于等于80分的学生信息
        Dataset<Row> studentScoreDF =  sqlContext.read().json("student.json");
        studentScoreDF.registerTempTable("student_scores");
        Dataset<Row> goodStudentScoresDF =  sqlContext.sql("select name,score from student_scores where scores >= 80");
        //取出其中的名字
        List<String> nameList = goodStudentScoresDF.javaRDD().map(new Function<Row, String>() {

            public String call(Row row) throws Exception {
                return row.getAs("name");
            }
        }).collect();

        List<String> studentInfoJson = new ArrayList<>();
        studentInfoJson.add("{\"name\":\"Yasaka\",\"age\":18}");
        studentInfoJson.add("{\"name\":\"Xuruyun\",\"age\":17}");
        studentInfoJson.add("{\"name\":\"Liangyongqi\",\"age\":19}");
        JavaRDD<String> studentInfoRDD = sc.parallelize(studentInfoJson);
        Dataset<Row>  studentInfoDF = sqlContext.read().json(studentInfoRDD);
        studentInfoDF.registerTempTable("student_infos");

        //根据nameList取出大于等于80的学生的信息
        String sql = "select name,age from student_infos where name in ( ";
        for(int i = 0; i < nameList.size() ; i++){
            sql+="'"+nameList.get(i)+"'";
            if(i < nameList.size()-1){
                sql+=",";
            }
        }
        sql+=")";
        Dataset<Row> goodStudentInfoDF = sqlContext.sql(sql);

        //join
       JavaPairRDD<String,Tuple2<Integer,Integer>> goodStudentRDD =  goodStudentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String,Integer>(String.valueOf(row.getAs("name")),Integer.valueOf(row.getAs("score")));
            }

        }).join(goodStudentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String,Integer>(String.valueOf(row.getAs("name")),Integer.valueOf(row.getAs("age")));
            }
        }));


       JavaRDD<Row> goodStudentROW =  goodStudentRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {

           public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
               return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
           }
       });

       List<StructField> fields = new ArrayList<>();
       fields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
       fields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
       fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));

        StructType structType =  DataTypes.createStructType(fields);
        Dataset<Row> goodStudentDF =  sqlContext.createDataFrame(goodStudentROW,structType);
        goodStudentDF.write().format("json").mode(SaveMode.Overwrite).save("");
    }
}
