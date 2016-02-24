/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ejesposito.app;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.base.Optional;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/** Consumes messages from one or more topics in Kafka and does wordcount.
 *
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * To run this example:
 *   `$ bin/run-example org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \
 *    zoo03 my-consumer-group topic1,topic2 1`
 */

public final class TopTitles {
  private static final Pattern pattern = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

  static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());

  private TopTitles() {
  }

  public static void main(String[] args) {
    if (args.length < 4) {
      System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
      System.exit(1);
    }

    //Logger.getLogger("org").setLevel(Level.WARN);
    //Logger.getLogger("akka").setLevel(Level.WARN);

    // Spark conf
    //SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaKafkaWordCount").set("spark.streaming.receiver.maxRate","10000");
    SparkConf sparkConf = new SparkConf().setMaster("spark://ip-172-31-45-179:7077")
                                         .setAppName("JavaKafkaWordCount")
                                         .set("spark.streaming.receiver.maxRate","100000")
                                         .set("spark.akka.heartbeat.interval", "100")
                                         .set("spark.akka.frameSize", "20");
                                         //.set("spark.executor.memory", "4g")
                                         //.set("spark.driver.memory", "4g");
                                         //.setJars(jars);

    // Create the context with 2 seconds batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

    // Checkpoint of the context
    jssc.checkpoint("hdfs://ec2-52-87-202-245.compute-1.amazonaws.com:9000/");

    // Receiver Input DStream conf
    int numThreads = Integer.parseInt(args[3]);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    String[] topics = args[2].split(",");
    for (String topic: topics) {
      topicMap.put(topic, numThreads);
    }
    JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

    // Get lines from the input stream
    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });

    // Count the number of on time departure fligths per airport and carrier
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      public Iterable<String> call(String x) {
        String[] data = pattern.split(x);
        if (data.length == 5) {
          if (data[3].matches("[-+]?\\d*\\.?\\d+")) {
            if (Double.compare(Double.parseDouble(data[3]), Double.parseDouble("15.00")) < 0) {
              return Arrays.asList(data[1] + "," + data[0]);
            } else {
    	      return Arrays.asList();
            }
          } else {
            return Arrays.asList();
          }
        } else {
          return Arrays.asList();
        }
      }
    });

    // mapToPair <"Airport,Carrier", 1> -> reduceByKey -> updateStateByKey <"Airport,Carrier", sum>
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
        public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
          int newSum = state.or(0);
          for (int i : values) {
            newSum += i;
          }
          return Optional.of(newSum);
        }
      });

    // <"Airport,Carrier>, sum> transform to <"Airport", "Carrier,sum">
    JavaPairDStream<String, String> carriersPerAirport = wordCounts.mapToPair(
      new PairFunction<Tuple2<String, Integer>, String, String>() {
        public Tuple2<String, String> call(Tuple2<String, Integer> tuple) throws Exception {
          try {
            String data[] = pattern.split(tuple._1());
            String value = String.valueOf(tuple._2());
            return new Tuple2<String, String>(data[0], data[1] + "," + value);
          } catch (Exception e) {
            return new Tuple2<String, String>("null", "null,0");
          }
        }
      });

    // Group by key "Airport"
    JavaPairDStream<String, Iterable<String>> grouped = carriersPerAirport.groupByKey();

    // Sort by "Carrier"
    JavaPairDStream<String, Iterable<String>> sorted = grouped.mapValues(
        new Function<Iterable<String>, Iterable<String>>() {
          public Iterable<String> call(Iterable<String> it) {
            List<String> newList = new ArrayList<String>();
            for (String i : it) {
              newList.add(i);
            }
            Collections.sort(newList, new Comparator<String>() {
              public int compare(String s1, String s2) {
                Integer valueS1 = Integer.parseInt(pattern.split(s1)[1]);
                Integer valueS2 = Integer.parseInt(pattern.split(s2)[1]);
                return (valueS1 > valueS2) ? 1 : (valueS2 > valueS1) ? -1 : 0;
              }
            });
            if (newList.size() >= 10) {
              newList = new ArrayList<String>(newList.subList(newList.size() - 10, newList.size()));
            }
            return newList;
          }
      });

      sorted.foreachRDD(
        new Function<JavaPairRDD<String,Iterable<String>>, Void>() {
          public Void call(JavaPairRDD<String, Iterable<String>> rdd) {
            rdd.foreachPartition(
              new VoidFunction<Iterator<Tuple2<String,Iterable<String>>>>() {
                public void call(Iterator<Tuple2<String,Iterable<String>>> it) {
                  //AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
                  try {
                    List<TopCarriersByAirport> list = new ArrayList<>();
                    DynamoDBMapper mapper = new DynamoDBMapper(client);
                    while(it.hasNext()) {
                      Tuple2<String,Iterable<String>> item = it.next();
                      TopCarriersByAirport top = new TopCarriersByAirport ();
                      top.airport = item._1().toString();
                      top.carriersList = new ArrayList<String> ();
                      for (String i : (Iterable<String>)item._2()) {
                        top.carriersList.add(i);
                      }
                      list.add(top);
                    }
                    mapper.batchSave(list);
                 } catch (Exception e) {
                 }
               }
             });
            return null;
          }
        });

    sorted.print();
    jssc.start();
    jssc.awaitTermination();
  }

   @DynamoDBTable(tableName="TopCarriersByAirport")
   public static class TopCarriersByAirport {
            private String airport;
            private List<String> carriersList;

            //Partition key
            @DynamoDBHashKey(attributeName="Airport")
            public String getAirport() { return airport; }
            public void setAirport(String airport) { this.airport = airport; }

            @DynamoDBAttribute(attributeName="CarriersList")
            public List<String> getCarriersList() { return carriersList; }
            public void setCarriersList(List<String> popularity) { this.carriersList = carriersList; }
     }

}
