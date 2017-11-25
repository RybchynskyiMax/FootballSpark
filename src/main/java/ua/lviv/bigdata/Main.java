package ua.lviv.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("footballCount");
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
        JavaPairRDD<String,Integer> nationalitiesMap = footballPlayersMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
         JavaPairRDD<String,Tuple2<Integer,Integer>> joinPlayersAndChampions = footballPlayersMap.join(championsMap);
         JavaRDD<Tuple2<Integer,Integer>> onlyNumOfPlayersAndNumOfWin = joinPlayersAndChampions.values();
         JavaPairRDD<Integer,Integer> onlyNumOfPlayersAndNumOfWinMap = onlyNumOfPlayersAndNumOfWin.mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
             @Override
             public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                 return new Tuple2<>(integerIntegerTuple2._2,integerIntegerTuple2._1);
             }
         });
        JavaPairRDD<Integer,Integer> resultMap = onlyNumOfPlayersAndNumOfWinMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        resultMap.saveAsTextFile("/Data/DataOut/SparkResult");

    }
}
