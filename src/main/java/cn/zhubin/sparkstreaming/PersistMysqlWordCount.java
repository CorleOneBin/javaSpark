package cn.zhubin.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import shapeless.Tuple;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;

public class PersistMysqlWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[1]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaDStream<String> lines = jssc.textFileStream("hdfs://node1:8020/wordcount_dir");
        JavaDStream<String> words =  lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairDStream<String,Integer> pairs =  words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });

        JavaPairDStream<String,Integer> wordcount =  pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordcount.print();


        /**
         * 遍历DStream里面的RDD，然后遍历里面的Partition而不是每个元素，这样能减少JDBC连接。
         */
        wordcount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

            public void call(JavaPairRDD<String, Integer> wordcountsRDD) throws Exception {
                wordcountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {

                    public void call(Iterator<Tuple2<String, Integer>> wordcounts) throws Exception {
                        Connection conn = ConnectionPool.getConnection();   //一个partiton 一个连接，从连接池获取

                        Tuple2<String,Integer> wordcount = null;
                        while(wordcounts.hasNext()){
                            wordcount = wordcounts.next();
                            String sql = "insert into wordcount(word,count) values ("
                                    + "'"+ wordcount._1 +"',"+wordcount._2+")";
                            Statement statement = conn.createStatement();
                            statement.executeUpdate(sql);
                            statement.close();
                        }
                        ConnectionPool.returnConnection(conn);
                    }

                });
            }

        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
