/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.inatel.demos;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class SpeedAvg {

  public static void main(String[] args) throws Exception {
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    DataStream stream = env.addSource(
            new FlinkKafkaConsumer09<>("flink-demo", 
            new JSONDeserializationSchema(), 
            properties)
    );
 

    stream  .flatMap(new TelemetryJsonParser())
            .keyBy(0)
            .timeWindow(Time.seconds(3))//"Janela" de conjunto de informações definida de 3 em 3 segundos.
            .reduce(new AvgReducer())
            .flatMap(new AvgMapper())
            .map(new AvgPrinter())
            .print();

    env.execute();
    }


    //Recebe o JSON do Kafka broker e converte car, gear e inicializa um contador no qual indicará a quantidade de vezes
    //que a marcha de cada carro for trocada
    static class TelemetryJsonParser implements FlatMapFunction<ObjectNode, Tuple3<String, Float, Integer>> {
      @Override
      public void flatMap(ObjectNode jsonTelemetry, Collector<Tuple3<String, Float, Integer>> out) throws Exception {
        String carNumber = "car" + jsonTelemetry.get("Car").asText();
        float gear = jsonTelemetry.get("telemetry").get("Gear").floatValue(); 
        out.collect(new Tuple3<>(carNumber,  gear, 1 ));
      }
    }

    // Reduce Function - Sum samples and count
    // Essa função compara, para cada carro, a marcha atual com a marcha anterior. Caso elas sejam diferentes, inclementa um contator.
    // TODO: Remover value1.f1 no retorno da tupla (remover definições de Tupla3 para Tupla2).
    static class AvgReducer implements ReduceFunction<Tuple3<String, Float, Integer>> {
      @Override
      public Tuple3<String, Float, Integer> reduce(Tuple3<String, Float,Integer> value1, Tuple3<String, Float, Integer> value2) {
        int isSameValue = Float.compare(value1.f1,value2.f1);
        if (isSameValue != 0) {
          value1.f2++;
        } 
        return new Tuple3<>(value1.f0, value1.f1, value1.f2);
      }
    }

    // FlatMap Function - Average
    // Envia para a classe AvgPrinter o carro em questão e a quantidade de vezes que a marcha foi trocada no intervalo (3 segundos no caso).
    static class AvgMapper implements FlatMapFunction<Tuple3<String, Float, Integer>, Tuple2<String, Integer>> {
      @Override
      public void flatMap(Tuple3<String, Float, Integer> carInfo, Collector<Tuple2<String, Integer>> out) throws Exception {
        out.collect(  new Tuple2<>( carInfo.f0 , carInfo.f2 )  );
      }
    }

    // Map Function - Print average
    //Exibe a string montada com os valores recebidos.  
    static class AvgPrinter implements MapFunction<Tuple2<String, Integer>, String> {
      @Override
      public String map(Tuple2<String, Integer> avgEntry) throws Exception {
        return  String.format("Marcha do carro %s trocada %d vezes em 3 segundos ", avgEntry.f0 , avgEntry.f1 ) ;
      }
    }

  }
