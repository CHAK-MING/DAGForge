# Changelog

All notable changes to DAGForge will be documented in this file.

## [0.1.0-beta] - Initial Beta Release

### Added
- **Core Architecture:**
  - High-performance DAG (Directed Acyclic Graph) workflow orchestrator built with C++23.
  - Seastar-inspired sharded async runtime minimizing lock contention.
  - TOML-based DAG definitions with hot-reload support.
- **Executors:**
  - `shell`: Native subprocess execution.
  - `docker`: Containerized task execution.
  - `sensor`: Polling tasks for files, HTTP endpoints, or shell commands.
- **Workflow Features:**
  - XCom cross-task communication via template variables (`{{ds}}`, `{{xcom_pull(...)}}`).
  - Branching DAGs via `is_branch = true` tasks.
  - Comprehensive trigger rules (`all_success`, `all_failed`, `one_success`, etc.).
  - Configurable retries, timeouts, and soft-fails.
- **Storage & State:**
  - Asynchronous MySQL persistence using `Boost.Mysql`.
  - Watermark-based crash recovery for orphaned tasks.
- **CLI & APIs:**
  - Full-featured CLI for service management, DAG triggering, and inspection.
  - HTTP REST API for programmatic control.
  - WebSocket API for real-time logs and task status events.
- **Web UI:**
  - Modern React 19 dashboard.
  - Real-time DAG visualization via React Flow.
  - Live log streaming and run history inspection.

### Deployment & Artifacts
- **Docker:** Official images available at `ghcr.io/<owner>/dagforge:0.1.0-beta`.
- **Prebuilt Linux Tarball:** Self-contained archive with binary, config, and web-ui distribution.

### Documentation
- Comprehensive `README.md` and `README_CN.md` with quickstart guides.
- Detailed `USER_GUIDE.md` covering all features and troubleshooting.
- Dedicated `API.md` for REST/WebSocket integrations.
