# Production-Grade Additions

This document tracks production-engineering work beyond the core application plan.
The goal: emulate what you'd actually do at a company shipping this to prod — all runnable locally or free-tier.

**Legend**
- ✅ Free / fully local — no cost
- 🟡 Free tier exists — watch limits
- 🔴 Paid — skip or stub

---

## 1. Infrastructure as Code (Terraform)

**What it shows:** You provision infra the same way every environment does it — no clicking in the console.

| Resource | Tool | Cost | Notes |
|---|---|---|---|
| Local infra (Postgres, Redis, Kafka) | `docker-compose.yml` | ✅ Free | Already planned |
| AWS VPC, subnets, security groups | Terraform | 🔴 Paid | Skip — use local |
| EKS / ECS cluster | Terraform | 🔴 Paid | Use local k8s instead |
| RDS PostgreSQL | Terraform | 🔴 Paid | Use local Postgres |
| ElastiCache Redis | Terraform | 🔴 Paid | Use local Redis |
| MSK (Managed Kafka) | Terraform | 🔴 Paid | Use local Kafka |
| **Terraform module structure** | Terraform | ✅ Free | Write it, just don't `apply` to AWS |
| **Remote state (local backend)** | Terraform | ✅ Free | Use local backend or free Terraform Cloud |
| S3 + DynamoDB state backend | Terraform | 🟡 Free tier | Negligible cost |

**Recommendation:** Write a full Terraform module tree (`modules/postgres`, `modules/redis`, `modules/kafka`, `modules/app`) targeting AWS, but use `terraform plan` only. The code is the artifact — it proves you know how to structure IaC.

### Directory structure to create
```
deployments/
└── terraform/
    ├── main.tf
    ├── variables.tf
    ├── outputs.tf
    ├── backend.tf
    └── modules/
        ├── networking/       # VPC, subnets, SGs
        ├── database/         # RDS / local Postgres
        ├── cache/            # ElastiCache / local Redis
        ├── messaging/        # MSK / local Kafka
        └── app/              # ECS/EKS task defs or k8s manifests
```

---

## 2. Kubernetes

**What it shows:** You know how to deploy, scale, and harden a service in k8s — the standard deployment target at most companies.

| Resource | Tool | Cost | Notes |
|---|---|---|---|
| Local cluster | `kind` or `minikube` | ✅ Free | Runs on your laptop |
| API + Worker Deployments | k8s manifests / Helm | ✅ Free | Standard |
| HorizontalPodAutoscaler (CPU/RPS) | k8s HPA | ✅ Free | Scales API pods |
| **KEDA** (Kafka consumer lag scaling) | KEDA | ✅ Free | Scales workers by lag — very impressive |
| PodDisruptionBudget | k8s PDB | ✅ Free | Guarantees availability during drains |
| NetworkPolicy (pod traffic rules) | k8s NetworkPolicy | ✅ Free | Restrict inter-pod traffic |
| ConfigMap + Secret | k8s | ✅ Free | Inject config without baking into image |
| Liveness + readiness probes | k8s | ✅ Free | Wired to `/health` and `/ready` |
| Resource requests + limits | k8s | ✅ Free | CPU/memory per container |
| Helm chart | Helm | ✅ Free | Package everything as a chart |
| Argo Rollouts (canary/blue-green) | Argo | ✅ Free | Install on local cluster |

### Directory structure to create
```
deployments/
└── k8s/
    ├── namespace.yaml
    ├── api/
    │   ├── deployment.yaml
    │   ├── service.yaml
    │   ├── hpa.yaml
    │   └── pdb.yaml
    ├── worker/
    │   ├── deployment.yaml
    │   ├── keda-scaledobject.yaml   ← scales on Kafka consumer lag
    │   └── pdb.yaml
    ├── configmap.yaml
    └── networkpolicy.yaml

deployments/
└── helm/
    └── notifyhub/
        ├── Chart.yaml
        ├── values.yaml
        ├── values-dev.yaml
        ├── values-prod.yaml
        └── templates/
            ├── deployment-api.yaml
            ├── deployment-worker.yaml
            ├── service.yaml
            ├── hpa.yaml
            ├── keda-scaledobject.yaml
            └── _helpers.tpl
```

### KEDA ScaledObject (high-value showcase)
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: notifyhub-worker
spec:
  scaleTargetRef:
    name: notifyhub-worker
  minReplicaCount: 1
  maxReplicaCount: 20
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka:29092
        consumerGroup: notifyhub-email-workers
        topic: notifyhub.notifications.email
        lagThreshold: "50"       # 1 pod per 50 unprocessed messages
```

This is exactly what production Kafka consumer autoscaling looks like.

---

## 3. Database

| Addition | Cost | Notes |
|---|---|---|
| **PgBouncer** (connection pooling) | ✅ Free | Add as a sidecar or separate container in docker-compose |
| Read replica (local) | ✅ Free | Second Postgres container in docker-compose, streaming replication |
| Migration as init container | ✅ Free | Run `migrate up` as a k8s init container, not in app startup |
| Automated backup script | ✅ Free | `pg_dump` to local volume, cron job |

PgBouncer is a quick win — add it to `docker-compose.yml` and route the app through it. Shows you understand connection exhaustion under load.

---

## 4. Observability (Local Stack)

Everything below runs locally for free.

| Addition | Tool | Cost | Notes |
|---|---|---|---|
| Metrics scraping | Prometheus | ✅ Free | Already in docker-compose |
| Dashboards | Grafana | ✅ Free | Already in docker-compose |
| **Grafana dashboard JSON** | Grafana | ✅ Free | Commit to `deployments/grafana/` |
| **Alertmanager** | Alertmanager | ✅ Free | Add to docker-compose |
| Alert rules | Prometheus | ✅ Free | `deployments/prometheus/alerts.yml` |
| Distributed tracing | **Jaeger** (local) | ✅ Free | Add to docker-compose, wire OTel exporter |
| Log aggregation | **Loki + Promtail** | ✅ Free | Add to docker-compose, view logs in Grafana |

### Grafana dashboards to build
- Notification throughput per channel (send rate, delivered, failed, dropped)
- p50/p95/p99 delivery latency per channel + provider
- Kafka consumer lag per group (linked to KEDA — shows why autoscaling fired)
- Circuit breaker state per provider (0=closed, 1=half-open, 2=open)
- Rate-limited notifications over time
- DLQ depth (should always be near zero)

### Alertmanager rules to write
```yaml
# deployments/prometheus/alerts.yml
- alert: KafkaConsumerLagHigh
  expr: notifyhub_kafka_consumer_lag > 1000
  for: 2m
  annotations:
    summary: "Consumer {{ $labels.group }} is lagging behind"

- alert: ProviderErrorRateHigh
  expr: rate(notifyhub_delivery_attempts_total{status="failed"}[5m]) > 0.1
  for: 1m

- alert: CircuitBreakerOpen
  expr: notifyhub_circuit_breaker_state == 2
  for: 0s
  annotations:
    summary: "Circuit breaker open for provider {{ $labels.provider }}"

- alert: DLQDepthSpike
  expr: notifyhub_kafka_consumer_lag{topic="notifyhub.dlq"} > 10
  for: 1m
```

### docker-compose additions
```yaml
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"   # Jaeger UI
      - "4317:4317"     # OTLP gRPC

  alertmanager:
    image: prom/alertmanager:latest
    ports: ["9093:9093"]
    volumes: [./deployments/alertmanager.yml:/etc/alertmanager/alertmanager.yml]

  loki:
    image: grafana/loki:latest
    ports: ["3100:3100"]

  promtail:
    image: grafana/promtail:latest
    volumes:
      - /var/log:/var/log
      - ./deployments/promtail.yml:/etc/promtail/config.yml
```

---

## 5. Security

| Addition | Cost | Notes |
|---|---|---|
| **gosec** (SAST) in CI | ✅ Free | `go install github.com/securego/gosec/v2/cmd/gosec@latest` |
| **Trivy** container scanning in CI | ✅ Free | `trivy image notifyhub-api:latest` |
| **TruffleHog** secrets scanning | ✅ Free | GitHub Action or local pre-commit hook |
| API key rotation endpoint | ✅ Free | `POST /internal/tenants/:id/api-keys/rotate` |
| Audit log table | ✅ Free | New migration: log every key issuance + rotation |
| Rate limit per API key | ✅ Free | Already planned (`api/middleware/ratelimit.go`) |
| TLS in local docker-compose | ✅ Free | Self-signed cert via `mkcert`, nginx sidecar |

---

## 6. CI/CD Pipeline

| Addition | Tool | Cost | Notes |
|---|---|---|---|
| Lint + test + build | GitHub Actions | ✅ Free | 2000 min/month on free tier |
| Docker build + push to GHCR | GitHub Actions | ✅ Free | GitHub Container Registry is free |
| Trivy image scan in CI | GitHub Actions | ✅ Free | |
| gosec SAST in CI | GitHub Actions | ✅ Free | |
| Integration tests (testcontainers) | GitHub Actions | ✅ Free | Containers spin up in the runner |
| **Atlantis** (Terraform PR workflow) | Self-hosted | ✅ Free | Run locally or on a free VM |
| Argo CD (GitOps) | Local k8s | ✅ Free | Deploy to local `kind` cluster from git |

### Pipeline stages
```
PR opened:
  lint (golangci-lint)
  → gosec (SAST)
  → unit tests (go test -race)
  → integration tests (testcontainers)
  → build docker image
  → trivy scan image
  → terraform plan (comment on PR)

Merge to main:
  → build + tag image → push to GHCR
  → helm upgrade (Argo CD syncs to local kind cluster)
  → smoke test (curl /health + send one canary notification)
```

---

## 7. Load Testing

| Addition | Tool | Cost | Notes |
|---|---|---|---|
| k6 load test script | k6 | ✅ Free | Already planned in `scripts/loadtest/` |
| k6 → InfluxDB → Grafana | All local | ✅ Free | Real-time load test dashboard |
| Baseline + stress + soak scenarios | k6 | ✅ Free | Three separate scripts |

### Test scenarios to write
- **Baseline:** 10 RPS sustained for 5 min — establish p99 latency
- **Stress:** Ramp from 10 → 500 RPS over 2 min — find the breaking point
- **Soak:** 50 RPS for 30 min — catch memory leaks, goroutine leaks, connection pool exhaustion

---

## 8. Operational Runbooks

Simple markdown files in `docs/runbooks/`. Shows you've thought about what happens when things go wrong.

| Runbook | What it covers |
|---|---|
| `dlq-overflow.md` | Why messages hit DLQ, how to inspect + replay them |
| `circuit-breaker-open.md` | Which provider failed, how to verify it's back, how to reset |
| `consumer-lag-spike.md` | How to check lag, scale workers manually, identify slow consumers |
| `db-connection-exhaustion.md` | How to diagnose via pg_stat_activity, PgBouncer pool stats |
| `rate-limit-incident.md` | How to temporarily lift a user's rate limit in Redis |

---

## Priority Order (what to build first)

1. **Kubernetes manifests + Helm chart** — highest visibility, most transferable skill
2. **KEDA ScaledObject for workers** — production Kafka autoscaling, very impressive
3. **Full observability stack** (Jaeger + Loki + Alertmanager) in docker-compose — shows you instrument everything
4. **Grafana dashboards** — tangible artifact, easy to screenshot/demo
5. **CI/CD pipeline** (GitHub Actions → GHCR → Argo CD → kind) — full GitOps loop
6. **Terraform modules** (write, don't apply) — shows IaC discipline
7. **PgBouncer** in docker-compose — quick win, real production concern
8. **Load test scenarios** (k6 + Grafana) — run it, screenshot the dashboard under load
9. **Runbooks** — soft skill that separates junior from senior engineers
10. **Security tooling** (gosec + Trivy in CI) — table stakes at any serious company
