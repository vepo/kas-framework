# Evaluation Test Execution Guide

## Kafka Adaptive Streams (KAS) Evaluation Framework

This guide provides step-by-step instructions for executing the experimental evaluation of KAS, replicating the results presented in the research paper.

---

## 📋 Prerequisites

### Hardware Requirements
| Component | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 4 cores | 8+ cores |
| RAM | 8 GB | 16+ GB |
| Disk | 50 GB free | 100 GB free (SSD preferred) |

### Software Requirements
- **Java**: OpenJDK 21.0.2+ 
- **Maven**: 3.9.9+
- **Docker**: Latest version with Docker Compose
- **Go**: 1.20+ (for Parquet tools)
- **OS**: Linux (tested on openSUSE, Ubuntu 20.04+)

---

## 🚀 Quick Start

```bash
# Clone the repository
git clone git@github.com:vepo/kas-framework.git
cd kas-framework

# Build the project
mvn clean package

# Start Kafka cluster
./scripts/start-kafka

# Run benchmark
./scripts/start-test.sh
```

---

## 🔧 Environment Setup

### 1. System Preparation

```bash
# Install system dependencies (openSUSE)
sudo zypper refresh
sudo zypper install -y git-core zip unzip docker docker-compose docker-compose-switch \
                      go go-doc

# For Ubuntu/Debian
# sudo apt update
# sudo apt install -y git zip unzip docker.io docker-compose golang
```

### 2. Docker Configuration

```bash
# Enable and start Docker
sudo systemctl enable docker
sudo systemctl start docker

# Add user to docker group
sudo usermod -G docker -a $USER
newgrp docker  # Apply group changes
```

### 3. Storage Setup (Cloud Instances)

For cloud VMs with attached volumes:

```bash
# Format and mount data disk
lsblk  # Identify data disk (e.g., /dev/vdb)
sudo mkfs.ext4 /dev/vdb
sudo mkdir /mnt/docker-data
sudo mount /dev/vdb /mnt/docker-data
```

### 4. Development Tools

```bash
# SDKMAN for Java/Maven management
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

# Install Java and Maven
sdk install java 21.0.2-open
sdk install maven 3.9.9

# Parquet-tools for result analysis
export PATH=$PATH:$HOME/go/bin
echo 'export PATH=$PATH:$HOME/go/bin' >> ~/.bashrc
go install github.com/hangxie/parquet-tools@latest
```