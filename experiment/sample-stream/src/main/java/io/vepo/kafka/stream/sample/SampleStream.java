package io.vepo.kafka.stream.sample;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vepo.kafka.maestro.MaestroConfigs;
import io.vepo.kafka.maestro.MaestroStream;
import io.vepo.kafka.maestro.metrics.PerformanceOptimizer;
import io.vepo.kafka.stream.sample.SampleStream.TrainSpeed;
import io.vepo.kafka.stream.sample.serde.JsonSerde;
import io.vepo.kafka.stream.sample.serde.TrainSpeedSerde;
import io.vepo.maestro.experiment.data.TrainMoviment;

public class SampleStream {

    private static final Logger logger = LoggerFactory.getLogger(SampleStream.class);

    public record TrainSpeed(int departure, int arrival) {
    }

    public record Parameter(String key, int value) {
    }

    public static void main(String[] args) {
        Stream.of(new Parameter("max.partition.fetch.bytes", 1048576), // DISCARD (JIT Compiler)
                  new Parameter("max.partition.fetch.bytes", 524288), // -50%
                  new Parameter("max.partition.fetch.bytes", 576717), // -45%
                  new Parameter("max.partition.fetch.bytes", 629146), // -40%
                  new Parameter("max.partition.fetch.bytes", 681574), // -35%
                  new Parameter("max.partition.fetch.bytes", 734003), // -30%
                  new Parameter("max.partition.fetch.bytes", 786432), // -25%
                  new Parameter("max.partition.fetch.bytes", 838861), // -20%
                  new Parameter("max.partition.fetch.bytes", 891290), // -15%
                  new Parameter("max.partition.fetch.bytes", 943718), // -10%
                  new Parameter("max.partition.fetch.bytes", 996147), // -5%
                  new Parameter("max.partition.fetch.bytes", 1048576), // 0%
                  new Parameter("max.partition.fetch.bytes", 1101005), // 5%
                  new Parameter("max.partition.fetch.bytes", 1153434), // 10%
                  new Parameter("max.partition.fetch.bytes", 1205862), // 15%
                  new Parameter("max.partition.fetch.bytes", 1258291), // 20%
                  new Parameter("max.partition.fetch.bytes", 1310720), // 25%
                  new Parameter("max.partition.fetch.bytes", 1363149), // 30%
                  new Parameter("max.partition.fetch.bytes", 1415578), // 35%
                  new Parameter("max.partition.fetch.bytes", 1468006), // 40%
                  new Parameter("max.partition.fetch.bytes", 1520435), // 45%
                  new Parameter("max.partition.fetch.bytes", 1572864), // 50%
                  new Parameter("fetch.max.bytes", 26214400), // -50%
                  new Parameter("fetch.max.bytes", 28835840), // -45%
                  new Parameter("fetch.max.bytes", 31457280), // -40%
                  new Parameter("fetch.max.bytes", 34078720), // -35%
                  new Parameter("fetch.max.bytes", 36700160), // -30%
                  new Parameter("fetch.max.bytes", 39321600), // -25%
                  new Parameter("fetch.max.bytes", 41943040), // -20%
                  new Parameter("fetch.max.bytes", 44564480), // -15%
                  new Parameter("fetch.max.bytes", 47185920), // -10%
                  new Parameter("fetch.max.bytes", 49807360), // -5%
                  new Parameter("fetch.max.bytes", 52428800), // 0%
                  new Parameter("fetch.max.bytes", 55050240), // 5%
                  new Parameter("fetch.max.bytes", 57671680), // 10%
                  new Parameter("fetch.max.bytes", 60293120), // 15%
                  new Parameter("fetch.max.bytes", 62914560), // 20%
                  new Parameter("fetch.max.bytes", 65536000), // 25%
                  new Parameter("fetch.max.bytes", 68157440), // 30%
                  new Parameter("fetch.max.bytes", 70778880), // 35%
                  new Parameter("fetch.max.bytes", 73400320), // 40%
                  new Parameter("fetch.max.bytes", 76021760), // 45%
                  new Parameter("fetch.max.bytes", 78643200), // 50%
                  new Parameter("max.poll.records", 250), // -50%
                  new Parameter("max.poll.records", 275), // -45%
                  new Parameter("max.poll.records", 300), // -40%
                  new Parameter("max.poll.records", 325), // -35%
                  new Parameter("max.poll.records", 350), // -30%
                  new Parameter("max.poll.records", 375), // -25%
                  new Parameter("max.poll.records", 400), // -20%
                  new Parameter("max.poll.records", 425), // -15%
                  new Parameter("max.poll.records", 450), // -10%
                  new Parameter("max.poll.records", 475), // -5%
                  new Parameter("max.poll.records", 500), // 0%
                  new Parameter("max.poll.records", 525), // 5%
                  new Parameter("max.poll.records", 550), // 10%
                  new Parameter("max.poll.records", 575), // 15%
                  new Parameter("max.poll.records", 600), // 20%
                  new Parameter("max.poll.records", 625), // 25%
                  new Parameter("max.poll.records", 650), // 30%
                  new Parameter("max.poll.records", 675), // 35%
                  new Parameter("max.poll.records", 700), // 40%
                  new Parameter("max.poll.records", 725), // 45%
                  new Parameter("max.poll.records", 750), // 50%
                  new Parameter("receive.buffer.bytes", 32768), // -50%
                  new Parameter("receive.buffer.bytes", 36045), // -45%
                  new Parameter("receive.buffer.bytes", 39322), // -40%
                  new Parameter("receive.buffer.bytes", 42598), // -35%
                  new Parameter("receive.buffer.bytes", 45875), // -30%
                  new Parameter("receive.buffer.bytes", 49152), // -25%
                  new Parameter("receive.buffer.bytes", 52429), // -20%
                  new Parameter("receive.buffer.bytes", 55706), // -15%
                  new Parameter("receive.buffer.bytes", 58982), // -10%
                  new Parameter("receive.buffer.bytes", 62259), // -5%
                  new Parameter("receive.buffer.bytes", 65536), // 0%
                  new Parameter("receive.buffer.bytes", 68813), // 5%
                  new Parameter("receive.buffer.bytes", 72090), // 10%
                  new Parameter("receive.buffer.bytes", 75366), // 15%
                  new Parameter("receive.buffer.bytes", 78643), // 20%
                  new Parameter("receive.buffer.bytes", 81920), // 25%
                  new Parameter("receive.buffer.bytes", 85197), // 30%
                  new Parameter("receive.buffer.bytes", 88474), // 35%
                  new Parameter("receive.buffer.bytes", 91750), // 40%
                  new Parameter("receive.buffer.bytes", 95027), // 45%
                  new Parameter("receive.buffer.bytes", 98304), // 50%
                  new Parameter("send.buffer.bytes", 65536), // -50%
                  new Parameter("send.buffer.bytes", 72090), // -45%
                  new Parameter("send.buffer.bytes", 78643), // -40%
                  new Parameter("send.buffer.bytes", 85197), // -35%
                  new Parameter("send.buffer.bytes", 91750), // -30%
                  new Parameter("send.buffer.bytes", 98304), // -25%
                  new Parameter("send.buffer.bytes", 104858), // -20%
                  new Parameter("send.buffer.bytes", 111411), // -15%
                  new Parameter("send.buffer.bytes", 117965), // -10%
                  new Parameter("send.buffer.bytes", 124518), // -5%
                  new Parameter("send.buffer.bytes", 131072), // 0%
                  new Parameter("send.buffer.bytes", 137626), // 5%
                  new Parameter("send.buffer.bytes", 144179), // 10%
                  new Parameter("send.buffer.bytes", 150733), // 15%
                  new Parameter("send.buffer.bytes", 157286), // 20%
                  new Parameter("send.buffer.bytes", 163840), // 25%
                  new Parameter("send.buffer.bytes", 170394), // 30%
                  new Parameter("send.buffer.bytes", 176947), // 35%
                  new Parameter("send.buffer.bytes", 183501), // 40%
                  new Parameter("send.buffer.bytes", 190054), // 45%
                  new Parameter("send.buffer.bytes", 196608) // 50%
        ).forEach(parameter -> {
            startKafka();
            createTopics();
            startAddDelay();
            var producerPid = startProducer();
            startStream(parameter);
            shutdown(producerPid);
        });
    }

    private static void startAddDelay() {
	logger.info("Starting Add Delay");
        try {
            var p = Runtime.getRuntime()
                           .exec(new String[] {
                               "./scripts/add-delay.sh"
                           });
            int exitCode = p.waitFor();
	    logger.info("Add delay exitCode={}", exitCode);
            if (exitCode != 0) {
                shutdown(null);
            }
            Thread.sleep(Duration.ofSeconds(2).toMillis());
        } catch (IOException e) {
            logger.error("Error initializing add delay", e);
            shutdown(null);
        } catch (InterruptedException e) {
            shutdown(null);
            Thread.currentThread().interrupt();
        }
    }

    private static final AtomicInteger producerCounter = new AtomicInteger(0);

    private static List<Process> startProducer() {
        var execution = producerCounter.incrementAndGet();
	logger.info("Starting {} producer!", execution);
        return IntStream.range(0, 5)
                        .mapToObj(i -> {
                            try {
                                return new ProcessBuilder("mvn",
                                                          "-pl",
                                                          "experiment/data-generator/",
                                                          "compile",
                                                          "exec:java",
                                                          "-Dexec.mainClass=io.vepo.kafka.stream.datagenerator.InjectData",
                                                          "-Dexec.args=-d ./experiment/train-data -t 10 -r 20000 train")
                                                                                                                        .redirectError(new File(String.format("log/producer-%03d-%02d.err.log", execution, i)))
                                                                                                                        .redirectOutput(new File(String.format("log/producer-%03d-%02d.log", execution, i)))
                                                                                                                        .start();
                            } catch (IOException e) {
                                logger.error("Error initializing inject data", e);
                                shutdown(null);
                                return null;
                            }
                        }).toList();
    }

    private static void createTopics() {
	logger.info("Creating topics");
        try {
            var p = Runtime.getRuntime()
                           .exec(new String[] {
                               "./scripts/create-topics"
                           });

            int exitCode = p.waitFor();
	    logger.info("Creating topics exitCode={}", exitCode);
            if (exitCode != 0) {
                shutdown(null);
            }
            Thread.sleep(Duration.ofSeconds(2).toMillis());
        } catch (IOException e) {
            logger.error("Error creating topic", e);
            shutdown(null);
        } catch (InterruptedException e) {
            shutdown(null);
            Thread.currentThread().interrupt();
        }
    }

    private static void startKafka() {
	logger.info("Starting Kafka");
        try {
            var p = Runtime.getRuntime()
                           .exec(new String[] {
                               "./scripts/start-kafka"
                           });
            int exitCode = p.waitFor();
	    logger.info("Start Kafka exitCode={}", exitCode);
            if (exitCode != 0) {
                shutdown(null);
            }
            Thread.sleep(Duration.ofSeconds(2).toMillis());
        } catch (IOException e) {
            logger.error("Error creating Kafka", e);
            shutdown(null);
        } catch (InterruptedException e) {
            shutdown(null);
            Thread.currentThread().interrupt();
        }
    }

    private static void shutdown(List<Process> producerPid) {
	logger.info("Shutdown environment");
        try {
            if (Objects.nonNull(producerPid)) {
                producerPid.forEach(Process::destroy);
            }
            var p = Runtime.getRuntime()
                           .exec(new String[] {
                               "./scripts/clean-environment"
                           });
            int exitCode = p.waitFor();
	    logger.info("Shutdown evironment exitCode={}", exitCode);
            if (exitCode != 0) {
                shutdown(null);
            }
            Thread.sleep(Duration.ofSeconds(2).toMillis());
        } catch (IOException e) {
            logger.error("Error cleaning up", e);
            shutdown(null);
        } catch (InterruptedException e) {
            shutdown(null);
            Thread.currentThread().interrupt();
        }
    }

    private static void startStream(Parameter parameter) {
	logger.info("Starting Streamer");
        var builder = new StreamsBuilder();
        builder.<String, TrainMoviment>stream("train.moviment")
               .groupByKey()
               .windowedBy(TimeWindows.of(Duration.ofMinutes(2)))
               .aggregate(
                          () -> new TrainSpeed(0, 0),
                          (key, value, aggregate) -> switch (value.eventType()) {
                              case "DEPARTURE" -> new TrainSpeed(aggregate.departure() + 1, aggregate.arrival());
                              case "ARRIVAL" -> new TrainSpeed(aggregate.departure(), aggregate.arrival() + 1);
                              default -> aggregate;
                          },
                          Materialized.<String, TrainSpeed, WindowStore<Bytes, byte[]>>as("train-speed-store")
                                      .withKeySerde(Serdes.String())
                                      .withValueSerde(new TrainSpeedSerde()))
               .toStream()
               // .peek((key, value) -> System.out.println("Key: " + key + " Value: " + value))
               .to("train.event-summary", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), new TrainSpeedSerde()));

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:9092,kafka-1:9094,kafka-2:9096");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "load-balancing-0");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(MaestroConfigs.MAESTRO_PARAMETER_NAME_CONFIG, parameter.key());
        props.put(MaestroConfigs.MAESTRO_PARAMETER_VALUE_CONFIG, parameter.value());
        var maestroStream = MaestroStream.create(builder.build(), props);
        var countDown = new CountDownLatch(1);
        maestroStream.cleanUp();
        PerformanceOptimizer.collecting.set(false);
        var taskExecutor = Executors.newSingleThreadScheduledExecutor();

        taskExecutor.schedule(() -> {
            PerformanceOptimizer.collecting.set(true);
        }, 5, TimeUnit.MINUTES);
        taskExecutor.schedule(() -> {
            PerformanceOptimizer.collecting.set(false);
            maestroStream.close();
            countDown.countDown();
        }, 30, TimeUnit.MINUTES);
        maestroStream.start();
        try {
            countDown.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
	logger.info("Streamer stopped");
    }
}
