# DESIGN.md - Scalehub Playbook Architecture

## Overview

Scalehub is a comprehensive infrastructure automation and experimentation platform that uses Ansible playbooks to manage the deployment and lifecycle of distributed systems infrastructure. The system is designed to provision, deploy, and manage complex multi-tier applications with monitoring, load generation, and data processing capabilities.

## Playbook Architecture

The playbook system is organized into three main tiers that follow a hierarchical deployment pattern:

### 1. Infrastructure Tier (`playbooks/infrastructure/`)
**Purpose**: Provisions and configures the foundational infrastructure layer
- **Main Playbook**: `setup.yaml`
- **Execution Order**: First (executed during provision or infrastructure setup)
- **Responsibilities**:
  - Cloud platform provisioning
  - Base system configuration
  - Network setup
  - Security configuration
  - Storage provisioning

### 2. Orchestration Tier (`playbooks/orchestration/`)
**Purpose**: Sets up container orchestration and cluster management
- **Main Playbook**: `setup.yaml`
- **Execution Order**: Second (after infrastructure)
- **Responsibilities**:
  - Kubernetes cluster deployment
  - Container runtime configuration
  - Cluster networking
  - Service mesh setup
  - Load balancer configuration

### 3. Application Tier (`playbooks/application/`)
**Purpose**: Deploys application components and services
- **Main Playbook**: `setup.yaml`
- **Execution Order**: Third (after orchestration)
- **Individual Application Playbooks**:
  - `base.yaml` - Base application services
  - `kafka.yaml` - Apache Kafka message broker
  - `flink.yaml` - Apache Flink stream processing
  - `monitoring.yaml` - Monitoring stack (Prometheus, Grafana, etc.)
  - `storage.yaml` - Persistent storage solutions
  - `network.yaml` - Network policies and configurations
  - `load_generators.yaml` - Load generator testing tools for Kafka and Flink
  - `data-stream-apps.yaml` - Data streaming applications
  - `ysb.yaml` - Yahoo Streaming Benchmark
  - `transscale.yaml` - Transscale application
  - `chaos.yaml` - Chaos engineering tools
  - `goldpinger.yaml` - Network connectivity testing
  - `gitlab.yaml` - GitLab CI/CD platform

## Execution Order and Flow

### 1. Full Provisioning Flow (`shub provision`)

1. Infrastructure Setup └── playbooks/infrastructure/setup (tag: create)
2. Lazy Setup (if enabled) ├── Orchestration Setup │ └── playbooks/orchestration/setup (tag: create) └── Application Setup └── playbooks/application/setup (tag: create)


### 2. Individual Playbook Operations
- **Deploy**: `shub deploy <playbook>` (tag: create)
- **Delete**: `shub delete <playbook>` (tag: delete)
- **Reload**: `shub reload <playbook>` (delete + create)

### 3. Destruction Flow (`shub destroy`)

1. Platform destruction (reverse order)
2. Cleanup of runtime configurations
3. Resource deallocation



## Playbook Categories

### Core Infrastructure Playbooks
- **infrastructure/setup**: Foundation layer provisioning
- **orchestration/setup**: Container orchestration setup

### Application Service Playbooks
- **application/base**: Essential application services
- **application/storage**: Persistent storage solutions
- **application/network**: Network configuration and policies
- **application/monitoring**: Observability stack

### Data Processing Playbooks
- **application/kafka**: Message streaming platform
- **application/flink**: Stream processing engine
- **application/data-stream-apps**: Custom streaming applications
- **application/ysb**: Yahoo Streaming Benchmark
- **application/load_generators**: Load testing infrastructure. Inject load into kafka for flink to process

### Testing and Validation Playbooks
- **application/chaos**: Chaos engineering tools
- **application/goldpinger**: Network connectivity validation

### Development and CI/CD Playbooks
- **application/gitlab**: Source control and CI/CD platform
- **application/transscale**: Application scaling experiments

## Configuration Management

### Execution Tags
- **create**: Deploy/install components
- **delete**: Remove/cleanup components
- **reload**: Delete and recreate components

### Special Handling
- **load_generators**: Has special handling via `role_load_generators()` method
- **lazy_setup**: Enables sequential deployment of all tiers during provisioning

## Design Principles

1. **Layered Architecture**: Infrastructure → Orchestration → Applications
2. **Idempotent Operations**: All playbooks support create/delete/reload operations
3. **Modular Design**: Each service has its own dedicated playbook
4. **Configuration-driven**: All deployments controlled via YAML configuration
5. **Experiment-ready**: Built-in support for load testing and benchmarking
6. **Observable**: Comprehensive monitoring and logging integration

## Dependencies and Prerequisites

1. **Infrastructure** must be provisioned before orchestration
2. **Orchestration** must be ready before application deployment
3. **Base applications** should be deployed before specialized services
4. **Monitoring** should be deployed early for observability
5. **Storage** must be available before stateful applications

This architecture enables scalable, reproducible deployments of complex distributed systems with comprehensive testing and monitoring capabilities.