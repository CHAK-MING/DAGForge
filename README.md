> [!NOTE]
> **DAGForge is in Active Development.**
> We are building a high-performance DAG workflow orchestrator built with modern C++23, inspired by the architecture of Apache Airflow but optimized for performance and low-latency execution.

<div align="center">

# DAGForge

[![DAGForge Web UI](./image/web-ui.png)](#)

</div>

> **Say goodbye to Python-induced lock contention and latency.**
> DAGForge uses a Seastar-inspired sharded async runtime where each CPU core has its own `io_context` (Boost.Asio) and memory resource.
>
> We believe workflow orchestration shouldn't be the bottleneck. DAGForge provides a blazing fast DAG engine with TOML-based definitions, async persistence, and a modern React 19 dashboard.

<div align="center">

[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg?style=flat-square&logo=c%2B%2B)](https://en.cppreference.com/w/cpp/23)
[![License](https://img.shields.io/badge/license-Apache--2.0-white?labelColor=black&style=flat-square)](LICENSE)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/CHAK-MING/dagforge)

[English](README.md) | [简体中文](README_CN.md)

</div>

---

## ✨ Key Features

- **Sharded Runtime:** Core-local `io_context` via Boost.Asio to minimize lock contention.
- **DAG Engine:** TOML-based DAGs supporting dependencies, trigger rules, branching, and sensors.
- **Executors:** Native support for Shell, Docker, and Sensor execution modes.
- **XCom:** Cross-task communication mechanism using template variables (e.g., `{{ds}}`, `{{xcom_pull(...)}}`).
- **Web UI:** Real-time visualization and management powered by React 19, Tailwind CSS, and React Flow.

## 📚 Documentation

### Getting Started
- **[Quickstart Guide](docs/USER_GUIDE.md#1-first-time-setup)** - Get up and running quickly.
- **[Detailed User Guide](docs/USER_GUIDE.md)** - In-depth usage, patterns, and troubleshooting.
- **[Configuration Guide](docs/USER_GUIDE.md#2-running-the-service)** - Settings and customization for the runtime.

### Core Features
- **[Trigger Rules](docs/USER_GUIDE.md#5-trigger-rules--when-to-use-each)** - Control when tasks become eligible to run.
- **[XCom Examples](docs/USER_GUIDE.md#6-xcom--complete-examples)** - Share data between tasks via MySQL `xcom_values`.
- **[Sensor Tasks](docs/USER_GUIDE.md#7-sensor-tasks)** - Block and poll on external conditions.
- **[Docker Tasks](docs/USER_GUIDE.md#8-docker-tasks)** - Run tasks inside isolated Docker containers.
- **[Branching DAGs](docs/USER_GUIDE.md#10-branching-dags)** - Conditional logic paths within pipelines.

### Integration
- **[API Reference](docs/API.md)** - HTTP REST and WebSocket API endpoints.
- **[Docker Deployment](docs/USER_GUIDE.md#8-docker-tasks)** - Simple `docker-compose` orchestration.

### Troubleshooting
- **[Troubleshooting Guide](docs/USER_GUIDE.md#16-troubleshooting)** - Common issues and solutions.

---

## 🚀 Quickstart (Minimal)

### 1) Download Release Package (Recommended)

```bash
# Download from GitHub Releases (replace version as needed)
curl -LO https://github.com/CHAK-MING/dagforge/releases/download/0.1.0-beta/dagforge-0.1.0-beta-linux-x86_64.tar.gz
tar -xzf dagforge-0.1.0-beta-linux-x86_64.tar.gz
cd dagforge-0.1.0-beta-linux-x86_64
```

Binary: `./bin/dagforge`

### 2) Prepare MySQL

```sql
-- Run as MySQL root
CREATE DATABASE dagforge CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'dagforge'@'%' IDENTIFIED BY 'dagforge';
GRANT ALL PRIVILEGES ON dagforge.* TO 'dagforge'@'%';
FLUSH PRIVILEGES;
```

### 3) Configure

```bash
cp system_config.toml my_config.local.toml
export DAGFORGE_CONFIG=my_config.local.toml
```

### 4) Init DB + Validate DAGs

```bash
dagforge db init
dagforge validate
```

### 5) Start Service (Release Package)

```bash
dagforge serve start

# Optional overrides
dagforge serve start --log-level debug --shards 4
# Daemon mode
dagforge serve start --daemon --log-file dagforge.log
```

API/UI: `http://127.0.0.1:8888`

### 6) Trigger and Inspect

```bash
dagforge trigger hello_world --wait
dagforge inspect hello_world --latest
dagforge logs hello_world --latest
```

### 7) Alternative: Build From Source

```bash
cmake --preset default
cmake --build --preset default
./build/bin/dagforge serve start
```

### 8) Alternative: Docker Compose

```bash
docker compose up -d
docker compose logs -f dagforge
```

---

## 💻 CLI Cheatsheet

```bash
# Service
dagforge serve start  [-c file] [--daemon/-d] [--log-file path] [--no-api] [--log-level trace|debug|info|warn|error] [--shards N]
dagforge serve status [-c file] [--json]
dagforge serve stop   [-c file] [--timeout N] [--force]

# Trigger & Test
dagforge trigger <dag_id> [--wait] [-e execution_date] [--no-api] [--json]
dagforge test <dag_id> <task_id> [--json]

# Listing
dagforge list dags  [--include-stale] [--limit N] [--json]
dagforge list runs  [dag_id] [--state failed|success|running] [--limit N] [--json]
dagforge list tasks [dag_id] [--json]

# Inspection and Logs
dagforge inspect <dag_id> [--run id|--latest] [--xcom] [--details] [--json]
dagforge logs <dag_id> [--run id|--latest] [--task task_id] [--attempt N] [-f|--follow] [--short-time] [--json]

# DAG Control
dagforge pause <dag_id> [--json]
dagforge unpause <dag_id> [--json]
dagforge clear <dag_id> --run <run_id> [--task id|--failed|--all] [--downstream] [--json]

# Database
dagforge db init
dagforge db migrate
dagforge db prune-stale [--dry-run]

# Validate
dagforge validate [-c file | -f dag.toml] [--json]
```

---

## 🗺️ Official Roadmap

See what's coming next for DAGForge:

1. **Enhance API Security:** Implement authentication and authorization mechanisms.
2. **PostgreSQL Support:** Add support for PostgreSQL alongside MySQL.
3. **Streamlined Configuration:** Support more efficient one-click deployments.
4. **Extended executor support:** Add more types of executors, such as a native Kubernetes (k8s) executor, to enable large-scale horizontal scalability.
5. **Observability:** Deep integration with OpenTelemetry and improved metrics.
6. **Performance Optimization:** Ongoing work on the C++23 coroutine runtime to further drop latencies.

---

## 🤝 Contributing

We welcome contributions! DAGForge is fully open source (Apache 2.0), and we encourage the community to:

- Report bugs and suggest features.
- Improve documentation.
- Submit code improvements and optimization PRs.
- Create new Executors and Sensors.

Check our **[Official Roadmap](#-official-roadmap)** for planned features and priorities.

## 📎 Resources

- **[Changelog](CHANGELOG.md)** - See recent notable updates.
- **[GitHub Issues](https://github.com/CHAK-MING/dagforge/issues)** - Report bugs or request features.

## 📄 Legal

- **License:** [Apache License 2.0](LICENSE)
