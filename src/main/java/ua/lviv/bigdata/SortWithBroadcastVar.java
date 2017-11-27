package ua.lviv.bigdata;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;


public class SortWithBroadcastVar {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("footballWithBroadcastVar");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> footballPlayers = sc.textFile("/Data/DataIn/Football/epldata_final.csv");
        HashMap<String,Integer> champions = new HashMap<>();
        champions.put("Brazil",5);
        champions.put("Germany",4);
        champions.put("Italy",4);
        champions.put("Argentina",2);
        champions.put("Uruguay",2);
        champions.put("France",1);
        champions.put("Spain",1);
        Broadcast<Map<String,Integer>> championsBroadcast = sc.broadcast(champions);
        JavaPairRDD<String, Integer> footballPlayersMap = footballPlayers.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] words=s.split(",");
                return new Tuple2<>(words[11],1);
            }
        });
        JavaPairRDD<String,Integer> nationalitiesMap = footballPlayersMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        JavaPairRDD<Integer,Integer> resultMap = nationalitiesMap.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
             if(championsBroadcast.value().containsKey(stringIntegerTuple2._1)){
                 return new Tuple2<>(championsBroadcast.value().get(stringIntegerTuple2._1),stringIntegerTuple2._2);
             }
             return new Tuple2<>(0,stringIntegerTuple2._2);
            }
        });
        resultMap = resultMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        resultMap.saveAsTextFile("/Data/DataOut/SparkFootballBroadcastVar");
    }
}
