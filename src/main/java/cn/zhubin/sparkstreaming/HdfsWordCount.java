package cn.zhubin.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class HdfsWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //这里不是用receiver来接收，所以不用单独占用一个线程，用一个线程也能让程序跑起来

        JavaDStream<String> lines = jssc.textFileStream("hdfs://node1:8020/wordcount_dir");



        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }

        });

        JavaPairDStream<String,Integer> pairs =  words.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }

        });

        JavaPairDStream<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }

        });
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();


    }
}
