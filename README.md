# Confluent Apache Kafka for Developers

Source code and hands-on exercises for the **Confluent Apache Kafka for Developers** course.

---

## Overview

This repository contains producer and consumer applications in multiple languages (Java, Python, .NET) that simulate a real-time driver tracking system. Messages containing driver GPS coordinates are produced to Kafka topics and consumed for visualization.

## Project Structure

```
├── challenge/          # Exercise scaffolding for students
├── solution/           # Complete solutions for each exercise
├── webserver/          # Real-time visualization dashboard
├── webserver-avro/     # Dashboard with Avro support
├── webserver-streams/  # Dashboard for Kafka Streams
├── postgres/           # Database initialization scripts
├── vol/                # Configuration files
└── docker-compose.yml  # Infrastructure setup
```

### Applications by Language

| Language | Producer | Consumer | Avro Producer | Avro Consumer | Streams |
|----------|----------|----------|---------------|---------------|---------|
| **Java** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Python** | ✅ | ✅ | ✅ | ✅ | — |
| **.NET** | ✅ | ✅ | ✅ | ✅ | — |

---

## Prerequisites

- **Docker** and **Docker Compose**
- **Java 21** (for Java exercises)
- **Python 3.10+** (for Python exercises)
- **.NET 8 SDK** (for .NET exercises)

---

## Getting Started

### 1. Start the Infrastructure

```bash
docker compose up -d
```

This starts:
- **Kafka** (KRaft mode) — `localhost:9092`
- **Schema Registry** — `localhost:8081`
- **Confluent Control Center** — `localhost:9021`
- **PostgreSQL** — `localhost:5432`
- **Prometheus** & **Grafana** for monitoring

### 2. Verify Services

```bash
docker compose ps
```

All services should show as `running` or `healthy`.

### 3. Access Control Center

Open [http://localhost:9021](http://localhost:9021) in your browser.

---

## Running the Exercises

### Java Applications

```bash
cd solution/java-producer
./gradlew build
./gradlew run
```

For Avro-based applications, the Gradle build automatically generates classes from `.avsc` schemas.

### Python Applications

```bash
cd solution/python-producer
python main.py
```

### .NET Applications

```bash
cd solution/dotnet-producer
dotnet run
```

---

## Exercise Modules

| Module | Description |
|--------|-------------|
| `*-producer` | Basic Kafka producer sending driver coordinates |
| `*-consumer` | Basic Kafka consumer reading driver positions |
| `*-producer-avro` | Producer using Avro serialization with Schema Registry |
| `*-consumer-avro` | Consumer using Avro deserialization |
| `*-consumer-prev` | Consumer with specific offset/partition handling |
| `java-streams-avro` | Kafka Streams application for real-time processing |

---

## Visualization Dashboard

The webserver displays real-time driver positions on a map:

```bash
docker compose up webserver -d
```

Open [http://localhost:3000](http://localhost:3000) to view the dashboard.

---

## VS Code Setup

For Java projects with Avro, build the project first to generate Avro classes:

```bash
cd solution/java-producer-avro
./gradlew build
```

Then in VS Code, run **"Java: Clean Java Language Server Workspace"** to refresh the project.

---

## Troubleshooting

### Avro class not found (Java)

Run `./gradlew build` to generate Avro classes from schemas.

### Connection refused to Kafka

Ensure Docker containers are running: `docker compose ps`

### Schema Registry errors

Verify Schema Registry is healthy:
```bash
curl http://localhost:8081/subjects
```

---

## Shutting Down

```bash
docker compose down
```

To remove all data volumes:
```bash
docker compose down -v
```

---

## License

This material is provided as part of Confluent training courses.
