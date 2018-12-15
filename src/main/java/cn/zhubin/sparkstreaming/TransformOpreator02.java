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

public class TransformOpreator02 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("local").setMaster("local");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        List<Tuple2<String, Boolean>> blackList = new ArrayList<>();
        blackList.add(new Tuple2<String,Boolean>("xiaoming",true));
        blackList.add(new Tuple2<String,Boolean>("hanmeimei",false));

        JavaPairRDD<String,Boolean> blackRDD = jssc.sparkContext().parallelizePairs(blackList);

        JavaReceiverInputDStream<String> adsClickDStream = jssc.socketTextStream("node1", 8888);
        JavaPairDStream<String, String> adsClickPairDStream  = adsClickDStream.mapToPair(new PairFunction<String, String, String>() {

            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[2], s);
            }

        });

        JavaDStream<String> validStream = adsClickPairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {

            public JavaRDD<String> call(JavaPairRDD<String, String> userLogBatchRDD) throws Exception {

                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRDD = userLogBatchRDD.leftOuterJoin(blackRDD);
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = joinRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {

                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        if (tuple._2._2.isPresent() && tuple._2._2.get()) {
                            return true;
                        }
                        return false;
                    }

                });

                JavaRDD<String> valuidRDD =  filterRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {

                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._2._1;
                    }
                });

                return valuidRDD;

            }

        });

        validStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
