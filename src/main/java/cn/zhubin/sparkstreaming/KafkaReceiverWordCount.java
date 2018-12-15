package cn.zhubin.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bouncycastle.crypto.PasswordConverter;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KafkaReceiverWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String,Integer> kafkaParams = new HashMap<>();
        kafkaParams.put("zhubin",1);  //topic 以及用几个线程数来接受Kafka数据

        String zkList = "node1:2181,node2:2181,node3:2181";

        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, zkList, "wordkcount", kafkaParams);

        //从kafka读进来是键值对，键没用
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
