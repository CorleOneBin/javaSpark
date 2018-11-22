package cn.zhubin.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class WindowBasedTopWord {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("WindowBasedTopWord").setMaster("local[2]")
                .set("spark.default.parallelism","100");   //并行度，reduceyBykey之后，使用这个作为并行度
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.checkpoint(".");   //reduceBykeyandwindow 使用的是第二种API，防止重复计算，所以要加这个

        //log日志  yasaka hello  ,  xuruyun word
        JavaReceiverInputDStream<String> log = jssc.socketTextStream("node1",8888);
        JavaDStream<String> searchWord =  log.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                return s.split(" ")[1];
            }
        });

        JavaPairDStream<String,Integer> pairs = searchWord.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }

        });

        JavaPairDStream<String,Integer> wordCounts =  pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }

        }, new Function2<Integer, Integer, Integer>() {   //可选参数，可以防止重复计算，设置为相反的逻辑

            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1-v2;
            }

        },Durations.seconds(60),Durations.seconds(10));   //第三个参数是窗口的长度，第四个参数是窗口间隔多少秒进行一次计算。  必须都是batch的整数倍，即5的倍数

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
