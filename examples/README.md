# Goccia Examples

This directory contains examples for the [goccia](https://github.com/FerroO2000/goccia) library.

It also includes a Makefile for managing the examples from the current directory.

## Overview

Each example has its own set of commands for managing Docker containers, building, and running applications. All examples share a common telemetry infrastructure that can be started and stopped independently. The metrics and traces can be visulized using **grafana** at `http://localhost:3000` in the "Goccia" dashboard.

## General Usage
```bash
make <example> <command> [arguments]
```

---

## Available Examples

### CAN

Manages CAN (Controller Area Network) server and client applications.

The server listens for Cannelloni encoded messages over UDP, then it decodes them and stores them in Quest DB. Its pipeline is composed of 6 stages:

1. UDP Ingress
2. Cannelloni Decoder
3. ROB (ReOrder Buffer)
4. CAN
5. Custom Processor (CAN to Quest DB messages)
6. Quest DB Egress

The client sends to the listening server some _dummy_ Cannelloni encoded CAN messages. Each message is generated randomly every millisecond. Its pipeline is composed of 4 stages:

1. Ticker Ingress
2. Custom Processor (generates the Cannelloni message)
3. Cannelloni Encoder
4. UDP Egress

#### Commands
```bash
# Start telemetry and CAN Docker containers
make can up

# Stop telemetry and CAN Docker containers
make can down

# Run the CAN server
make can server

# Run the CAN client
make can client
```

---

### CSV

Manages CSV data processing application. The program will read a CSV file from the data/in folder and encode its parsed copy to a new CSV file in the data/out folder. It is composed of 4 stages:

1. File Ingress
2. CSV Decoder
3. CSV Encoder
4. File Egress

#### Commands
```bash
# Start telemetry infrastructure
make csv up

# Stop telemetry infrastructure
make csv down

# Run the CSV application
make csv run
```

---

### eBPF

> [!IMPORTANT]
> The eBPF example requires linux and root privileges.

Manages eBPF (Extended Berkeley Packet Filter) application with network interface monitoring. The application will attach an eBPF program to the network interface (XDP) and will log the received ping packets. It is composed of 3 stages:

1. eBPF Ingress
2. Custom Processor (prints the received ping packets)
3. Sink Egress (ignores the messages)

#### Commands
```bash
# Start telemetry infrastructure
make ebpf up

# Stop telemetry infrastructure
make ebpf down

# Build the eBPF application
make ebpf build

# Run the eBPF application with default interface (eth0)
sudo make ebpf run

# Run the eBPF application with custom interface
sudo make ebpf run <interface_name>

# Example:
sudo make ebpf run wlan0
```

---

### File

Demonstrates the usage of the file ingress stage. The application will read each file (.txt) in the data/in folder and merge them into a single file in the data/out folder. The pipeline is composed of 3 stages:

1. File Ingress
2. Custom Processor (appends the file name to each line)
3. File Egress

#### Commands
```bash
# Start telemetry infrastructure
make file up

# Stop telemetry infrastructure
make file down

# Appends test data to the file every 0.1 seconds (default file_name=input.txt)
make file gen <file_name>

# Example:
# Appends data to a file called test.txt into the data/in folder
make file gen test.txt

# Run the file processing application
make file run
```

---

### Kafka Example

Manages Kafka producer and consumer applications.

The producer sends to the Kafka broker some _dummy_ messages. Each message is generated randomly every millisecond. Its pipeline is composed of 3 stages:

1. Ticker Ingress
2. Custom Processor (generates the Kafka message)
3. Kafka Egress

The consumer reads messages from the Kafka broker and stores them into another topic. Its pipeline is analogous to the producer.

#### Commands
```bash
# Start telemetry and Kafka Docker containers
make kafka up

# Stop telemetry and Kafka Docker containers
make kafka down

# Run the Kafka producer
make kafka producer

# Run the Kafka consumer
make kafka consumer
```
