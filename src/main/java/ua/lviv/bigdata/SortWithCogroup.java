package ua.lviv.bigdata;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class SortWithCogroup {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("footballWithCogroup");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> footballPlayers = sc.textFile("/Data/DataIn/Football/epldata_final.csv");
        JavaRDD<String> champions = sc.textFile("/Data/CacheFiles/champions.csv");
        JavaPairRDD<String, Integer> footballPlayersMap = footballPlayers.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] words=s.split(",");
                return new Tuple2<>(words[11],1);
            }
        });
        JavaPairRDD<String, Integer> championsMap = champions.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] words=s.split(",");
                return new Tuple2<>(words[0],Integer.parseInt(words[1]));


            }
        });
        JavaPairRDD<String,Tuple2<Iterable<Integer>,Iterable<Integer>>> cogroupPlayersAndChampions = footballPlayersMap.cogroup(championsMap);
//
        JavaPairRDD<Integer,Integer> onlyNumOfPlayersAndNumOfWinMap = cogroupPlayersAndChampions.values().mapToPair(new PairFunction<Tuple2<Iterable<Integer>, Iterable<Integer>>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Iterable<Integer>, Iterable<Integer>> iterableIterableTuple2) throws Exception {
                int sum =0;
                if(!Iterables.isEmpty(iterableIterableTuple2._2)){
                    for (Integer a_1 : iterableIterableTuple2._1) {
                        sum += a_1;
                    }
                    return new Tuple2<>(iterableIterableTuple2._2.iterator().next(),sum);
                    }
                for (Integer a_1 : iterableIterableTuple2._1) {
                    sum += a_1;
                }
                return new Tuple2<>(0,sum);
                }
        });
        JavaPairRDD<Integer,Integer> resultMap = onlyNumOfPlayersAndNumOfWinMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        resultMap.saveAsTextFile("/Data/DataOut/SparkFootballCogroup");
    }
}