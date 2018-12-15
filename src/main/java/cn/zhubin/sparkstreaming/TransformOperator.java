package cn.zhubin.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 把数据直接和一个黑名单RDD进行join操作而过滤黑名单的人员
 * 直接RDD和RDD操作，不需要针对每一个元素
 */

public class TransformOperator {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("TransformOperator").setMaster("local[2]");
            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(20));

        //用户对于网站上的广告可以进行点击，点击之后进行实时计算，但是有些用户刷广告，需要过滤
        //要有一个黑名单机制！只要是黑名单中的用户点击的广告，我们就给过滤掉

        //先来模拟一个黑名单数据RDD，true代表启用，false代表不启用。 当然，没在黑名单内的人不需要过滤
        List<Tuple2<String,Boolean>> blackList = new ArrayList<>();
        blackList.add(new Tuple2<String,Boolean>("xiaoming",true));
        blackList.add(new Tuple2<String,Boolean>("hanmeimei",false));

        JavaPairRDD<String, Boolean> blackListRDD =  jssc.sparkContext().parallelizePairs(blackList);

        //time adId name  ：输入的数据
        JavaReceiverInputDStream<String> adsClickLogDStream = jssc.socketTextStream("node1",8888);

        //根据name来判断是否在黑名单内，所以name为key
        JavaPairDStream<String,String> adsClickLogPairDStream =  adsClickLogDStream.mapToPair(new PairFunction<String, String, String>() {

            public Tuple2<String, String> call(String line) throws Exception {
                return new Tuple2<String,String>(line.split(" ")[2],line);
            }

        });

        JavaDStream<String> validStream = adsClickLogPairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {

            public JavaRDD<String> call(JavaPairRDD<String, String> userLogBatchRDD) throws Exception {

                //进行左外连接，把数据和黑名单以name为key进行join。  所有的log日志都会有
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = userLogBatchRDD.leftOuterJoin(blackListRDD);

                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = joinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {

                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        if (tuple._2._2.isPresent() && tuple._2._2.get()) {  //判断这个是黑名单里面的
                            return false;
                        }
                        return true;
                    }
                });

                JavaRDD<String> validLogRDD = filterRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {

                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._2._1;
                    }
                });

                return validLogRDD;
            }
        });

        validStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
