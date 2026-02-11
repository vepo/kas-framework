# Kafka Adaptive Streams (KAS)

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**KAS** is a self-adaptive framework for Apache Kafka Streams applications that dynamically tunes configuration parameters to maximize throughput under resource constraints. Operating within the same JVM as your application, KAS continuously monitors system metrics and applies coordinated adaptation rules without requiring external controllers.

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Adaptation Rules](#adaptation-rules)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Metrics Collection](#metrics-collection)
- [Experimental Results](#experimental-results)
- [Contributing](#contributing)
- [Citation](#citation)
- [License](#license)

## 🔭 Overview

Data Stream Processing systems face significant operational challenges due to fluctuating workloads, dynamic resource availability, and environmental variability. Static configurations often lead to throughput degradation, increased latency, and unbounded consumer lag.

**KAS** addresses these challenges by embedding a lightweight, in-JVM adaptation engine that:

- Continuously monitors system and environmental metrics
- Classifies operational state (sustainable/unsustainable throughput, resource availability)
- Dynamically adjusts key Kafka Streams parameters through complementary adaptation rules
- Achieves throughput improvements of up to **123%** (passthrough) and **270%** (aggregation) in controlled experiments

## ✨ Key Features

- **🎯 Self-Optimization**: Automatically maximizes throughput while respecting CPU and memory constraints
- **⚡ In-JVM Operation**: No external dependencies or sidecar containers required
- **🧩 Four Complementary Rules**: Thread Allocation, Fetch Adjustment, Compression, and Micro-batching
- **📊 Trend-Based Decision Making**: Uses historical metric analysis, not instantaneous values
- **🔄 Topology-Aware**: Adapts behavior based on processing topology characteristics
- **🚀 Production Ready**: Lightweight, proven in experimental Kafka Streams workloads

## 🏗️ Architecture

KAS implements the MAPE-K (Monitor-Analyze-Plan-Execute) feedback loop directly within your Kafka Streams application:

```
┌─────────────────────────────────────────────────────────────┐
│                    JVM / Kafka Streams                      │
│  ┌─────────────┐     ┌───────────────────────────────────┐  │
│  │   Managed   │     │          Managing System          │  │
│  │   System    │     │  ┌─────────┐  ┌───────────────┐   │  │
│  │ ┌─────────┐ │     │  │ Monitor │  │   Analyzer    │   │  │
│  │ │Kafka    │ │<────┼──┤ Metrics │  │(State Class.) │   │  │
│  │ │Streams  │ │     │  └─────────┘  └───────┬───────┘   │  │
│  │ │App      │ │     │                       │           │  │
│  │ └────┬────┘ │     │  ┌──────────┐  ┌──────v──────┐    │  │
│  │      │      │     │  │ Executor │<─┤   Planner   │    │  │
│  │      │      │     │  │ (Apply   │  │  (Rule      │    │  │
│  │ ┌────v────┐ │     │  │  Rules)  │  │   Selection)│    │  │
│  │ │Config   │ │     │  └──────────┘  └─────────────┘    │  │
│  │ │Updates  │ │     └───────────────────────────────────┘  │
│  │ └─────────┘ │                                            │
│  └─────────────┘                                            │
└─────────────────────────────────────────────────────────────┘
                           │
                    ┌──────v──────┐
                    │ Environment │
                    │ (OS, CPU,   │
                    │  Memory,    │
                    │  Network)   │
                    └─────────────┘
```

KAS collects metrics via:
- **Kafka MetricsReporter interface** – Consumer/producer metrics
- **JVM core libraries** – Memory pools, GC activity
- **Linux system information** – CPU utilization, available memory

## 🧠 Adaptation Rules

KAS implements four complementary adaptation rules, each targeting a distinct system bottleneck:

| Rule | Target | Description |
|------|--------|-------------|
| **Thread Allocation** | CPU/Memory | Dynamically creates processing threads when resources are available and throughput is unsustainable |
| **Fetch Adjustment** | Network I/O | Optimizes `max.partition.fetch.bytes` based on partition allocation per thread |
| **Compression** | Network Payload | Enables optimal compression algorithm when producer compression is disabled |
| **Micro-batching** | Network Efficiency | Tunes `linger.ms` to increase batch sizes and reduce thread blocking |

### Rule 1: Thread Allocation

Creates new processing threads when:
- System has available CPU **and** memory capacity
- Throughput is classified as **unsustainable** (increasing consumer lag trend)

```java
// Maximum allocatable threads determined by bottleneck resource
N_max = min(N_c, N_m)
```

### Rule 2: Fetch Adjustment

When external reconfiguration (e.g., HPA scaling) changes partition assignment, KAS recalculates the optimal `max.partition.fetch.bytes`:

```java
// Average partitions per fetch request
N_ppf = min(ceil(N_assigned / N_brokers), N_assigned)

// Optimal per-partition fetch size
F_partition = F_max / N_ppf
```

### Rule 3: Compression

When producer `compression.type=none`, KAS automatically:
1. Detects broker compression configuration
2. Enables the same algorithm (defaults to `snappy` if broker also has no compression)
3. Eliminates unnecessary broker decompression/recompression overhead

### Rule 4: Micro-batching

When processing threads experience blocking due to:
- Full send buffer
- Max in-flight requests reached

KAS increases `linger.ms` to accumulate records before transmission, with an upper bound derived from available buffer capacity:

```java
Δt_linger = (B_avail / (R · S_record)) × 1000
```

## 📦 Installation

### Maven

```xml
<dependency>
    <groupId>dev.vepo.kafka</groupId>
    <artifactId>kafka-maestro</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

```gradle
implementation 'dev.vepo.kafka:kafka-maestro:0.1.0'
```

## 🚀 Quick Start

### 1. Replace `KafkaStreams` with `MaestroStreams`
```java
Properties props = new Properties();
// ... configure properties
KafkaStreams streams = new KafkaStreams(builder.build(), props);
streams.start();
```

## ⚙️ Configuration

### KAS-Specific Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `maestro.adapter.rules` | List of rule classes to enable | Empty (all adaptation rules) |

### Selecting Adaptation Rules

Enable specific rules based on your topology characteristics:

```java
// All rules - maximum throughput improvement
props.put(MaestroConfigs.ADAPTER_RULE_CLASSES_CONFIG,
          List.of(ThreadAllocationRule.class,
                  AdjustConsumerFetchSizeRule.class,
                  UseCompressionOnProducerRule.class,
                  BatchProducerRule.class));

// I/O bound topologies - focus on fetch and batching
props.put(MaestroConfigs.ADAPTER_RULE_CLASSES_CONFIG,
          List.of(AdjustConsumerFetchSizeRule.class,
                  BatchProducerRule.class));

// CPU bound topologies - focus on thread allocation
props.put(MaestroConfigs.ADAPTER_RULE_CLASSES_CONFIG,
          List.of(ThreadAllocationRule.class));
```

## 📊 Metrics Collection

KAS collects comprehensive metrics for both decision-making and post-experiment analysis:

```java
// Enable metrics collection
props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
          PerformanceMetricsCollector.class.getName());
props.put("metrics.stats.folder", "/opt/kas-metrics");
```

Metrics are written to CSV files organized by application ID and timestamp:

```
/opt/kas-metrics/
  └── my-app-2024-01-15-10-30-45/
      ├── consumer-metrics.csv
      ├── producer-metrics.csv
      ├── stream-thread-metrics.csv
      ├── state-store-metrics.csv
      └── system-metrics.csv
```

## 📈 Experimental Results

KAS has been experimentally validated against two representative topologies:

### Passthrough Topology
*Simple message forwarding - I/O bound*

| Configuration | Throughput Gain | Latency Impact | CPU Increase | Memory Increase |
|--------------|-----------------|----------------|--------------|-----------------|
| Thread Allocation | +43% | -17% | +18% | +2% |
| Fetch Adjustment | **+51%** | +1,501% | +58% | +0.7% |
| Compression | +20% | -9% | +4% | +0.3% |
| Micro-batching | +22% | +242% | +5% | +1.8% |
| **Full KAS** | **+123%** | +1,278% | +157% | +43% |

### Aggregation Topology
*Stateful windowed aggregation - CPU/network bound*

| Configuration | Throughput Gain | Latency Impact | CPU Increase | Memory Increase |
|--------------|-----------------|----------------|--------------|-----------------|
| Thread Allocation | +23% | -26% | +36% | +18% |
| Fetch Adjustment | -17% | +1,099% | +3% | +31% |
| Compression | **+159%** | -23% | +159% | +15% |
| Micro-batching | +64% | -35% | +56% | +2% |
| **Full KAS** | **+270%** | +305% | +253% | +26% |

### Key Findings

1. **Synergy beats isolation**: Full KAS consistently outperforms any single rule
2. **Topology matters**: Compression excels in aggregation; Fetch Adjustment excels in passthrough
3. **Controlled trade-offs**: Latency increases are acceptable when throughput gains enable lag control
4. **Resource-aware scaling**: Memory growth remains bounded by availability equations

## 🤝 Contributing

We welcome contributions! Areas of particular interest:

- **New adaptation rules**: Network-aware strategies, self-healing mechanisms
- **Alternative decision engines**: Reinforcement learning, fuzzy control
- **Broader ecosystem**: Kafka Connect, standalone consumers/producers
- **Extended evaluation**: Additional topologies, hardware platforms, workload patterns

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

Apache License 2.0

---

**KAS** is developed and maintained at the [Federal University of Technology – Paraná (UTFPR)](http://www.utfpr.edu.br), Graduate Program in Applied Computing (PPGCA).