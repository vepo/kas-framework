package io.vepo.kafka.stream.sample;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import io.vepo.kafka.maestro.MaestroConfigs;
import io.vepo.kafka.maestro.MaestroStream;
import io.vepo.kafka.maestro.metrics.PerformanceOptimizer;
import io.vepo.kafka.stream.sample.serde.JsonSerde;
import io.vepo.maestro.experiment.data.TrainMoviment;

public class SampleStream {

    public record TrainSpeed(int departure, int arrival) {
    }

    public record Parameter(String key, int value) {
    }

    public static void main(String[] args) {
        Stream.of(new Parameter("max.partition.fetch.bytes", 1048576), // DISCARD (JIT Compiler)
                  new Parameter("max.partition.fetch.bytes", 524288), // -50%
                  new Parameter("max.partition.fetch.bytes", 629145), // -40%
                  new Parameter("max.partition.fetch.bytes", 734000), // -30%
                  new Parameter("max.partition.fetch.bytes", 838860), // -20%
                  new Parameter("max.partition.fetch.bytes", 943718), // -10%
                  new Parameter("max.partition.fetch.bytes", 1048576), // 0%
                  new Parameter("max.partition.fetch.bytes", 1153433), // 10%
                  new Parameter("max.partition.fetch.bytes", 1258291), // 20%
                  new Parameter("max.partition.fetch.bytes", 1363148), // 30%
                  new Parameter("max.partition.fetch.bytes", 1468006), // 40%
                  new Parameter("max.partition.fetch.bytes", 1572864), // 50%
                  new Parameter("fetch.max.bytes", 26214400), // -50%
                  new Parameter("fetch.max.bytes", 31457280), // -40%
                  new Parameter("fetch.max.bytes", 36700160), // -30%
                  new Parameter("fetch.max.bytes", 41943040), // -20%
                  new Parameter("fetch.max.bytes", 47185920), // -10%
                  new Parameter("fetch.max.bytes", 52428800), // 0%
                  new Parameter("fetch.max.bytes", 57671680), // 10%
                  new Parameter("fetch.max.bytes", 62914560), // 20%
                  new Parameter("fetch.max.bytes", 68157440), // 30%
                  new Parameter("fetch.max.bytes", 73400320), // 40%
                  new Parameter("fetch.max.bytes", 78643200), // 50%
                  new Parameter("max.poll.records", 250), // -50%
                  new Parameter("max.poll.records", 300), // -40%
                  new Parameter("max.poll.records", 350), // -30%
                  new Parameter("max.poll.records", 400), // -20%
                  new Parameter("max.poll.records", 450), // -10%
                  new Parameter("max.poll.records", 500), // 0%
                  new Parameter("max.poll.records", 550), // 10%
                  new Parameter("max.poll.records", 600), // 20%
                  new Parameter("max.poll.records", 650), // 30%
                  new Parameter("max.poll.records", 700), // 40%
                  new Parameter("max.poll.records", 750), // 50%
                  new Parameter("receive.buffer.bytes", 32768), // -50%
                  new Parameter("receive.buffer.bytes", 39321), // -40%
                  new Parameter("receive.buffer.bytes", 45875), // -30%
                  new Parameter("receive.buffer.bytes", 52428), // -20%
                  new Parameter("receive.buffer.bytes", 58982), // -10%
                  new Parameter("receive.buffer.bytes", 65536), // 0%
                  new Parameter("receive.buffer.bytes", 72089), // 10%
                  new Parameter("receive.buffer.bytes", 78643), // 20%
                  new Parameter("receive.buffer.bytes", 85196), // 30%
                  new Parameter("receive.buffer.bytes", 91750), // 40%
                  new Parameter("receive.buffer.bytes", 98304), // 50%
                  new Parameter("send.buffer.bytes", 65536), // -50%
                  new Parameter("send.buffer.bytes", 78643), // -40%
                  new Parameter("send.buffer.bytes", 91750), // -30%
                  new Parameter("send.buffer.bytes", 104857), // -20%
                  new Parameter("send.buffer.bytes", 117964), // -10%
                  new Parameter("send.buffer.bytes", 131072), // 0%
                  new Parameter("send.buffer.bytes", 144179), // 10%
                  new Parameter("send.buffer.bytes", 157286), // 20%
                  new Parameter("send.buffer.bytes", 170393), // 30%
                  new Parameter("send.buffer.bytes", 183500), // 40%
                  new Parameter("send.buffer.bytes", 196608) // 50%
        ).forEach(parameter -> {
            startKafka();
            createTopics();
            var producerPid = startProducer();
            startStream(parameter);
            shutdown(producerPid);
        });
    }

    private static final AtomicInteger producerCounter = new AtomicInteger(0);

    private static Process startProducer() {
        try {
            return new ProcessBuilder(new String[] {
                "mvn",
                "-pl",
                "experiment/data-generator/",
                "compile",
                "exec:java",
                "-Dexec.mainClass=io.vepo.kafka.stream.datagenerator.InjectData",
                "-Dexec.args=-d ./experiment/train-data -t 20 -r 1000 train"
            }).redirectError(new File(String.format("producer-%02d.err.log", producerCounter.incrementAndGet())))
              .redirectOutput(new File(String.format("producer-%02d.log", producerCounter.get())))
              .start();
        } catch (IOException e) {
            shutdown(null);
            return null;
        }
    }

    private static void createTopics() {
        try {
            var p = Runtime.getRuntime()
                           .exec(new String[] {
                               "./scripts/create-topics"
                           });

            int exitCode = p.waitFor();
            if (exitCode != 0) {
                shutdown(null);
            }
            Thread.sleep(Duration.ofSeconds(2).toMillis());
        } catch (IOException e) {
            shutdown(null);
        } catch (InterruptedException e) {
            shutdown(null);
            Thread.currentThread().interrupt();
        }
    }

    private static void startKafka() {
        try {
            var p = Runtime.getRuntime()
                           .exec(new String[] {
                               "./scripts/start-kafka"
                           });
            int exitCode = p.waitFor();
            if (exitCode != 0) {
                shutdown(null);
            }
            Thread.sleep(Duration.ofSeconds(2).toMillis());
        } catch (IOException e) {
            shutdown(null);
        } catch (InterruptedException e) {
            shutdown(null);
            Thread.currentThread().interrupt();
        }
    }

    private static void shutdown(Process producerPid) {
        try {
            producerPid.destroy();
            var p = Runtime.getRuntime()
                           .exec(new String[] {
                               "./scripts/clean-environment"
                           });
            int exitCode = p.waitFor();
            if (exitCode != 0) {
                shutdown(null);
            }
            Thread.sleep(Duration.ofSeconds(2).toMillis());
        } catch (IOException e) {
            shutdown(null);
        } catch (InterruptedException e) {
            shutdown(null);
            Thread.currentThread().interrupt();
        }
    }

    private static void startStream(Parameter parameter) {
        var builder = new StreamsBuilder();
        builder.<String, TrainMoviment>stream("train.moviment")
               .groupByKey()
               .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
               .aggregate(() -> new TrainSpeed(0, 0),
                          (key, value, aggregate) -> switch (value.eventType()) {
                              case "DEPARTURE" -> new TrainSpeed(aggregate.departure() + 1, aggregate.arrival());
                              case "ARRIVAL" -> new TrainSpeed(aggregate.departure(), aggregate.arrival() + 1);
                              default -> aggregate;
                          },
                          (key, v1, v2) -> new TrainSpeed(v1.departure() + v2.departure(), v1.arrival() + v2.arrival()))
               .toStream()
               .to("train.event-summary", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class)));

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:9092,kafka-1:9094,kafka-2:9096");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "load-balancing-0");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(MaestroConfigs.MAESTRO_PARAMETER_NAME_CONFIG, parameter.key());
        props.put(MaestroConfigs.MAESTRO_PARAMETER_VALUE_CONFIG, parameter.value());
        var maestroStream = MaestroStream.create(builder.build(), props);
        var countDown = new CountDownLatch(1);
        maestroStream.cleanUp();
        PerformanceOptimizer.collecting.set(false);
        var taskExecutor = Executors.newSingleThreadScheduledExecutor();

        taskExecutor.schedule(() -> {
            PerformanceOptimizer.collecting.set(true);
        }, 10, TimeUnit.MINUTES);
        taskExecutor.schedule(() -> {
            maestroStream.close();
            countDown.countDown();
        }, 40, TimeUnit.MINUTES);
        maestroStream.start();
        try {
            countDown.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
