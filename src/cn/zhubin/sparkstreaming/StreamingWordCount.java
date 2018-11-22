package cn.zhubin.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

// yum install nc
   // nc -lk 8888
    //然后启动程序，在node1 输入数据就可以读取了

/**
 * 通过socket 来读某个端口的数据
 */

public class StreamingWordCount {
    public static void main(String[] args) throws InterruptedException {
        //这里一定要设置线程>=2 不然一个进程只能取信息，不能计算信息
        SparkConf conf = new SparkConf().setAppName("StreamingWordcount").setMaster("local[2]");
        /**
         * 创建该对象类似于spark core 中的JavaSparkContext，类似于SparkSQL中的SQLContext
         * 该对象除了接受SparkConf对象，还接收一个BatchInterval参数，就是说，每收集多长时间的数据划分为一个Batch即RDD去执行
         */
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 首先创建输入DStream,代表一个数据源比如这里从socket或Kafka来持续不断的进入实时数据流
         * 创建一个监听socket数据量，RDD里面的每一个元素就是一行行的文本
         * Receiver 会单独占用一个线程来读取数据,每时每刻都在接受数据，隔5秒切割一个RDD
         */


        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("node1",8888);
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

        //计算完成后，都打印一下这5秒钟的情况，只会打印前十行
        wordCounts.print();

        //每次都要写这三个
        jssc.start();
        jssc.awaitTermination();
        jssc.close();


    }

    public void method(){

    }
}
