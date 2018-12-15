package cn.zhubin.sparkstreaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 使用direct方式读取kafka数据
 */

public class KafkaDirectWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[1]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String,String> kafkaParams = new HashMap<String,String>();
        kafkaParams.put("metadata.broker.list","node1:2181,node2:2181,node3:2181");

        Set<String> topics = new HashSet<String>();
        topics.add("zhubin");

        JavaPairDStream<String,String> lines =  KafkaUtils.createDirectStream(
                jssc,
                String.class,                //读进来的键值对和解码方式， 一般用这几个参数就可以了
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
                );

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

            public Iterator<String> call(Tuple2<String, String> tuple) throws Exception {
                return Arrays.asList(tuple._2.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }

        });

        JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }

        });

        wordcounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();


    }
}
