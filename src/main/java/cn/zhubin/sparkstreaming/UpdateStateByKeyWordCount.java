package cn.zhubin.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class UpdateStateByKeyWordCount {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("UpdateStateByKeyWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint(".");  //这个必须要设置。。为了寻找上一个状态，缓存

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("node1",8888);

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

        /**
         * 这个算子能将所有时间间隔内的结果加起来
         * list对应某个key在这个时间间隔内的values集合，
         * state对应这个key之间的状态的value
         */
        JavaPairDStream<String,Integer> wordcounts = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            public Optional<Integer> call(List<Integer> list, Optional<Integer> state) throws Exception {
                /**
                 * Optionl其实有两个子类，一个子类是Some，一个是none
                 */
                Integer newValues = 0;
                if(state.isPresent()){
                    newValues = state.get();
                }
                for(Integer value : list){
                    newValues += value;
                }

                return Optional.of(newValues);
            }
        });

        wordcounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
