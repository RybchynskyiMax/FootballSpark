package ua.lviv.bigdata;

import com.google.common.collect.Iterables;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import scala.Option;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SortWithAccum {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("footballWithAccum");
        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator zeroStar = sc.sc().longAccumulator();
        LongAccumulator oneStar = sc.sc().longAccumulator();
        LongAccumulator twoStar = sc.sc().longAccumulator();
        LongAccumulator threeStar = sc.sc().longAccumulator();
        LongAccumulator fourStar = sc.sc().longAccumulator();
        LongAccumulator fiveStar = sc.sc().longAccumulator();
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
      cogroupPlayersAndChampions.values().foreach(iterableIterableTuple2 -> {
          Iterator <Integer> iterator = iterableIterableTuple2._1.iterator();
          if(!Iterables.isEmpty(iterableIterableTuple2._2)) {
              switch (iterableIterableTuple2._2.iterator().next()){
                  case 1:
                      while (iterator.hasNext()){
                          oneStar.add(iterator.next());
                      }
                      break;
                  case 2:
                      while (iterator.hasNext()){
                          twoStar.add(iterator.next());
                      }
                      break;

                  case 3:
                      while (iterator.hasNext()){
                          threeStar.add(iterator.next());
                      }
                      break;
                  case 4:
                      while (iterator.hasNext()){
                          fourStar.add(iterator.next());
                      }
                      break;
                  case 5:
                      while (iterator.hasNext()){
                          fiveStar.add(iterator.next());
                      }
                      break;
              }

          }else
              while (iterator.hasNext()){
                  zeroStar.add(iterator.next());
              }
        });
        HashMap<Integer,Long> result = new HashMap<>();
        result.put(0,zeroStar.sum());
        result.put(1,oneStar.value());
        result.put(2,twoStar.value());
        result.put(3,threeStar.value());
        result.put(4,fourStar.value());
        result.put(5,fiveStar.value());
        for (Map.Entry<Integer, Long> integerLongEntry : result.entrySet()) {
            System.out.println(integerLongEntry);
        }
    }
}
