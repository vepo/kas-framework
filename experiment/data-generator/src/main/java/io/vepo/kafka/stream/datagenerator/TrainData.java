package io.vepo.kafka.stream.datagenerator;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.vepo.maestro.experiment.data.TrainInfo;
import io.vepo.maestro.experiment.data.TrainMoviment;
import dev.vepo.stomp4j.StompClient;
import dev.vepo.stomp4j.UserCredential;

public class TrainData {
    private static final Logger logger = LoggerFactory.getLogger(TrainData.class);

    public static void main(String[] args) throws InterruptedException {

        // create an instance of the stomp client
        //loadData();

        readData();
    }

    private static void readData() {
        var objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        var storePath = Paths.get(".", "train-data");
        if (storePath.toFile().exists()) {
            var histogram = new TreeMap<String, AtomicInteger>();
            Stream.of(storePath.toFile()
                               .listFiles((dir, name) -> name.endsWith(".json")))
                  .map(file -> {
                      // System.out.println("Reading file: " + file);
                      try (var reader = new FileInputStream(file)) {
                          var data = new byte[reader.available()];
                          reader.read(data);
                          return new String(data);
                      } catch (Exception e) {
                          // e.printStackTrace();
                          return null;
                      }
                  })
                  .filter(Objects::nonNull)
                  .map(value -> {
                      try {
                          return objectMapper.readValue(value, TrainInfo[].class);
                      } catch (JsonProcessingException e) {
                          // e.printStackTrace();
                          // System.exit(1);
                          return null;
                      }
                  })
                  .filter(Objects::nonNull)
                  .flatMap(Stream::of)
                  .forEach(info -> {
                      Stream.of(TrainMoviment.class.getDeclaredMethods())
                            .filter(m -> m.getParameters().length == 0)
                            .forEach(field -> {
                                try {
                                    var fieldValue = field.invoke(info.body());
                                    if (Objects.nonNull(fieldValue) && !fieldValue.toString().isBlank()) {
                                        histogram.computeIfAbsent(field.getName(), key -> new AtomicInteger(0))
                                                 .incrementAndGet();
                                    }
                                } catch (IllegalAccessException | InvocationTargetException e) {
                                    // ignore
                                }

                            });
                  });
            // .forEach(info -> histogram.computeIfAbsent(info.body().trainId(), key -> new
            // AtomicInteger(0))
            // .incrementAndGet());
            histogram.forEach((k, v) -> logger.info("Key {} has {} records", k, v));
            logger.info("Total keys: {}", histogram.size());
        }
    }

    private static void loadData() {
        try (StompClient stompClient = new StompClient("ws://publicdatafeeds.networkrail.co.uk:61618", new UserCredential("osorio@alunos.utfpr.edu.br", "DKce,qg58;Vw$"))) {

            // connect to the endpoint
            var storePath = Paths.get(".", "train-data");
            if (!storePath.toFile().exists()) {
                storePath.toFile().mkdirs();
            }
            stompClient.connect();
            stompClient.subscribe("/topic/TRAIN_MVT_ALL_TOC", data -> {
                System.out.println("===== Received Data =====");
                System.out.println(data);
                // write data to file
                var file = storePath.resolve("train-data-" + System.currentTimeMillis() + ".json");
                System.out.println("Writing data to file: " + file);
                try (var writer = new FileOutputStream(file.toFile())) {
                    writer.write(data.getBytes());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            stompClient.join();

            // subscribe to a topic an suppose to receive a result of type Event
            // stompClient.subscribeToTopic("/topic/TRAIN_MVT_ALL_TOC", String.class,
            // (result, error) -> {

            // if (error != null) {
            // System.out.println("Got an error: " + error.getMessage());
            // } else if (result != null) {
            // System.out.println("Got result: " + result);
            // } else {
            // System.out.println("Received an empty result");
            // }
            // });
            // Thread.sleep(1000000000);
            // System.out.println("Closing client!");

            // // close the stomp client
            // stompClient.close();
        }
    }
}
