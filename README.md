# Distributed MapReduce Cluster

A production-grade, containerized, and fault-tolerant distributed MapReduce system built in Go, using gRPC for transport, MinIO (S3-compatible API) as a shared storage layer, and an isolated streaming runner for executing user-defined Map/Reduce scripts (e.g. Python scripts).

---

## Quickstart

### 1. Start the Cluster
Run the automation script from the repository root to compile the Go binaries, initialize environment configuration, and boot up the Docker Compose cluster (including workers, storage, and monitoring stack):

```bash
./deploy.sh
```

---

## System Architecture & Wire Diagram

The following diagram represents the network topology, component communication, and monitoring scrape-paths of the MapReduce cluster:

```mermaid
flowchart TD
    subgraph Client ["Client / App Space"]
        APP["user_application.py (Map/Reduce script)"]
    end

    subgraph Cluster ["MapReduce Compute Engine"]
        COORD["coordinator (gRPC Scheduler)"]
        W1["worker-replica-1"]
        W2["worker-replica-2"]
        W3["worker-replica-3"]
    end

    subgraph Storage ["Shared Storage Layer"]
        MINIO["MinIO Object Storage (S3 API)"]
    end

    subgraph Observability ["Observability Stack"]
        PROM["prometheus"]
        GRAF["grafana"]
    end

    %% Communication paths
    W1 <-->|1. Polls / Reports Tasks via gRPC| COORD
    W2 <-->|1. Polls / Reports Tasks via gRPC| COORD
    W3 <-->|1. Polls / Reports Tasks via gRPC| COORD

    W1 -.->|2. Spawns Subprocess| APP
    W2 -.->|2. Spawns Subprocess| APP
    W3 -.->|2. Spawns Subprocess| APP

    W1 ===>|3. Reads inputs & writes mr-intermediates| MINIO
    W2 ===>|3. Reads inputs & writes mr-intermediates| MINIO
    W3 ===>|3. Reads inputs & writes mr-intermediates| MINIO
    COORD ===>|3. Pre-uploads local splits| MINIO

    %% Scraping paths
    PROM -.->|Scrapes metrics:9091| COORD
    PROM -.->|Scrapes metrics:9092| W1
    PROM -.->|Scrapes metrics:9092| W2
    PROM -.->|Scrapes metrics:9092| W3
    GRAF ===>|Queries Data| PROM
```

---

## Observability & Monitoring

Once deployed via `./deploy.sh`, you can access the following local endpoints:

*   **Grafana Dashboards**: [http://localhost:3000](http://localhost:3000) (Login: `admin` / `admin`)
*   **Prometheus Metrics**: [http://localhost:9092](http://localhost:9092)
*   **MinIO Console UI**: [http://localhost:9001](http://localhost:9001) (Login: `minioadmin` / `minioadmin`)
*   **Coordinator gRPC**: `localhost:9090`
