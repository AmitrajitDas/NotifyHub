# NotifyHub Deployment Guide

This document covers two deployment strategies:

1. **AWS Free Tier** — a cost-effective setup for development, demos, and low-traffic use
2. **Production-Grade** — a scalable, HA architecture for real workloads

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Part 1: AWS Free Tier Deployment](#part-1-aws-free-tier-deployment)
  - [Architecture Overview](#free-tier-architecture-overview)
  - [Step 1: AWS Account Setup](#step-1-aws-account-setup)
  - [Step 2: Networking (VPC)](#step-2-networking-vpc)
  - [Step 3: Database (RDS PostgreSQL)](#step-3-database-rds-postgresql)
  - [Step 4: Cache (ElastiCache Redis)](#step-4-cache-elasticache-redis)
  - [Step 5: Message Queue (MSK Serverless / Self-Hosted)](#step-5-message-queue)
  - [Step 6: Container Registry (ECR)](#step-6-container-registry-ecr)
  - [Step 7: Compute (ECS Fargate / EC2)](#step-7-compute)
  - [Step 8: Load Balancer & DNS](#step-8-load-balancer--dns)
  - [Step 9: CI/CD Pipeline](#step-9-cicd-pipeline)
  - [Step 10: Monitoring](#step-10-monitoring)
  - [Cost Breakdown](#free-tier-cost-breakdown)
- [Part 2: Production Deployment](#part-2-production-deployment)
  - [Architecture Overview](#production-architecture-overview)
  - [Infrastructure as Code](#infrastructure-as-code)
  - [Compute Layer](#compute-layer)
  - [Data Layer](#data-layer)
  - [Networking & Security](#networking--security)
  - [Observability Stack](#observability-stack)
  - [CI/CD Pipeline](#production-cicd-pipeline)
  - [Disaster Recovery](#disaster-recovery)
  - [Cost Estimate](#production-cost-estimate)

---

## Prerequisites

- AWS account (12-month free tier eligibility for new accounts)
- AWS CLI v2 installed and configured (`aws configure`)
- Docker installed locally
- Terraform (optional, for IaC)
- Domain name (optional, for custom DNS)

---

## Part 1: AWS Free Tier Deployment

### Free Tier Architecture Overview

```
                    ┌─────────────┐
                    │  Route 53   │ (optional)
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │     ALB     │ (free tier: not included,
                    └──────┬──────┘  use EC2 direct or skip)
                           │
              ┌────────────┼────────────┐
              │                         │
       ┌──────▼──────┐          ┌──────▼──────┐
       │  EC2 (API)  │          │ EC2 (Worker)│
       │  t2.micro   │          │  t2.micro   │
       └──────┬──────┘          └──────┬──────┘
              │                         │
    ┌─────────┼─────────┬───────────────┤
    │         │         │               │
┌───▼───┐ ┌──▼──┐ ┌────▼────┐ ┌───────▼───────┐
│RDS PG │ │Redis│ │  Kafka  │ │ SES (email)   │
│db.t3  │ │(EC2)│ │  (EC2)  │ │ SNS (push)    │
│.micro │ │     │ │         │ └───────────────┘
└───────┘ └─────┘ └─────────┘
```

**Strategy:** Run everything on a single `t2.micro` EC2 instance using Docker Compose for the simplest free-tier approach. For a slightly better setup, use RDS free tier for PostgreSQL and run everything else on EC2.

---

### Step 1: AWS Account Setup

1. Create an AWS account at https://aws.amazon.com/free
2. Enable MFA on the root account
3. Create an IAM user with programmatic access for deployments:

```bash
aws iam create-user --user-name notifyhub-deploy
aws iam attach-user-policy --user-name notifyhub-deploy \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
aws iam create-access-key --user-name notifyhub-deploy
```

4. Configure AWS CLI:

```bash
aws configure --profile notifyhub
# Enter access key, secret key, region (us-east-1 recommended for most free tier services)
```

---

### Step 2: Networking (VPC)

Use the default VPC for free-tier simplicity, or create a dedicated one:

```bash
# Use default VPC (simplest)
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" \
  --query "Vpcs[0].VpcId" --output text)

# Create a security group for NotifyHub
aws ec2 create-security-group \
  --group-name notifyhub-sg \
  --description "NotifyHub security group" \
  --vpc-id $VPC_ID

# Allow inbound HTTP (API)
aws ec2 authorize-security-group-ingress \
  --group-name notifyhub-sg \
  --protocol tcp --port 8080 --cidr 0.0.0.0/0

# Allow SSH
aws ec2 authorize-security-group-ingress \
  --group-name notifyhub-sg \
  --protocol tcp --port 22 --cidr <YOUR_IP>/32
```

---

### Step 3: Database (RDS PostgreSQL)

RDS offers 12 months of free tier: `db.t3.micro`, 20 GB storage, PostgreSQL 16.

```bash
aws rds create-db-instance \
  --db-instance-identifier notifyhub-db \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --engine-version "16" \
  --master-username notifyhub \
  --master-user-password "<STRONG_PASSWORD>" \
  --allocated-storage 20 \
  --storage-type gp2 \
  --no-multi-az \
  --publicly-accessible \
  --backup-retention-period 7 \
  --vpc-security-group-ids <SG_ID>
```

After the instance is ready, run migrations:

```bash
# Get the endpoint
RDS_ENDPOINT=$(aws rds describe-db-instances \
  --db-instance-identifier notifyhub-db \
  --query "DBInstances[0].Endpoint.Address" --output text)

# Run migrations
DATABASE_URL="postgres://notifyhub:<PASSWORD>@${RDS_ENDPOINT}:5432/notifyhub?sslmode=require"
make migrate-up
```

---

### Step 4: Cache (ElastiCache Redis)

ElastiCache is **not** in the free tier. For free-tier, run Redis on the EC2 instance via Docker.

If budget allows (~$13/mo for `cache.t3.micro`):

```bash
aws elasticache create-cache-cluster \
  --cache-cluster-id notifyhub-redis \
  --cache-node-type cache.t3.micro \
  --engine redis \
  --engine-version "7.0" \
  --num-cache-nodes 1
```

**Free alternative:** Run Redis in Docker on the EC2 instance (covered in Step 7).

---

### Step 5: Message Queue

Amazon MSK (Managed Kafka) is **not** free tier eligible. Options:

#### Option A: Self-hosted Kafka on EC2 (free tier)

Run Kafka via Docker Compose on the same EC2 instance. This works for low traffic but is not recommended beyond demos.

#### Option B: Amazon SQS as an alternative (~free tier eligible)

SQS offers 1 million free requests/month. This would require code changes to replace the Kafka producer/consumer with SQS, so we'll stick with self-hosted Kafka for now.

---

### Step 6: Container Registry (ECR)

ECR free tier: 500 MB storage/month for 12 months.

```bash
# Create repositories
aws ecr create-repository --repository-name notifyhub/api
aws ecr create-repository --repository-name notifyhub/worker

# Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com

# Build and push images
docker build --target api -t notifyhub/api -f deployments/Dockerfile .
docker tag notifyhub/api:latest <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/notifyhub/api:latest
docker push <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/notifyhub/api:latest

docker build --target worker -t notifyhub/worker -f deployments/Dockerfile .
docker tag notifyhub/worker:latest <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/notifyhub/worker:latest
docker push <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/notifyhub/worker:latest
```

---

### Step 7: Compute

#### Option A: Single EC2 with Docker Compose (Simplest, True Free Tier)

This runs everything on one `t2.micro` instance (1 vCPU, 1 GB RAM). Tight but workable for demos.

```bash
# Create a key pair
aws ec2 create-key-pair --key-name notifyhub-key \
  --query "KeyMaterial" --output text > notifyhub-key.pem
chmod 400 notifyhub-key.pem

# Launch EC2 instance (Amazon Linux 2023, t2.micro)
aws ec2 run-instances \
  --image-id ami-0c02fb55956c7d316 \
  --instance-type t2.micro \
  --key-name notifyhub-key \
  --security-groups notifyhub-sg \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=notifyhub}]' \
  --user-data file://scripts/ec2-user-data.sh
```

Create `scripts/ec2-user-data.sh`:

```bash
#!/bin/bash
set -euo pipefail

# Install Docker
yum update -y
yum install -y docker git
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

# Install Docker Compose
DOCKER_COMPOSE_VERSION="v2.24.0"
curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Clone and start
cd /home/ec2-user
git clone <YOUR_REPO_URL> notifyhub
cd notifyhub
docker-compose -f deployments/docker-compose.yml up -d
```

**Important:** On `t2.micro` (1 GB RAM), you'll need to create a swap file and reduce Kafka memory:

```bash
# SSH into the instance
ssh -i notifyhub-key.pem ec2-user@<PUBLIC_IP>

# Create 2GB swap
sudo dd if=/dev/zero of=/swapfile bs=1M count=2048
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile swap swap defaults 0 0' | sudo tee -a /etc/fstab
```

Create a `deployments/docker-compose.freetier.yml` override to reduce memory:

```yaml
# Override for free-tier EC2 (1 GB RAM + 2 GB swap)
services:
  kafka:
    environment:
      KAFKA_HEAP_OPTS: "-Xmx256m -Xms128m"

  postgres:
    command: postgres -c shared_buffers=64MB -c work_mem=4MB

  redis:
    command: redis-server --maxmemory 64mb --maxmemory-policy allkeys-lru
```

Run with:

```bash
docker-compose -f deployments/docker-compose.yml \
  -f deployments/docker-compose.freetier.yml up -d
```

#### Option B: ECS Fargate (Not Free Tier, but Low Cost)

If you're past free tier or want a more managed experience, ECS Fargate is a clean option. See the production section below for ECS patterns.

---

### Step 8: Load Balancer & DNS

For free tier, skip ALB (it costs ~$16/mo) and access the API directly via the EC2 public IP:

```
http://<EC2_PUBLIC_IP>:8080
```

For a stable endpoint, allocate an Elastic IP (free when attached to a running instance):

```bash
ALLOCATION_ID=$(aws ec2 allocate-address --query "AllocationId" --output text)
aws ec2 associate-address --allocation-id $ALLOCATION_ID --instance-id <INSTANCE_ID>
```

**Optional:** Use Cloudflare (free tier) in front for DNS + free SSL:

1. Register your domain with any registrar
2. Add your domain to Cloudflare (free plan)
3. Create an A record pointing to your Elastic IP
4. Enable Cloudflare proxy for free SSL termination

---

### Step 9: CI/CD Pipeline

Use **GitHub Actions** (free for public repos, 2000 min/mo for private):

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy to AWS

on:
  push:
    branches: [main]

env:
  AWS_REGION: us-east-1
  ECR_REGISTRY: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.us-east-1.amazonaws.com

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.22"
      - run: make test
      - run: make lint

  build-and-deploy:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Login to ECR
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build and push API image
        run: |
          docker build --target api -t $ECR_REGISTRY/notifyhub/api:${{ github.sha }} \
            -f deployments/Dockerfile .
          docker push $ECR_REGISTRY/notifyhub/api:${{ github.sha }}

      - name: Build and push Worker image
        run: |
          docker build --target worker -t $ECR_REGISTRY/notifyhub/worker:${{ github.sha }} \
            -f deployments/Dockerfile .
          docker push $ECR_REGISTRY/notifyhub/worker:${{ github.sha }}

      - name: Deploy to EC2
        uses: appleboy/ssh-action@v1
        with:
          host: ${{ secrets.EC2_HOST }}
          username: ec2-user
          key: ${{ secrets.EC2_SSH_KEY }}
          script: |
            cd /home/ec2-user/notifyhub
            git pull origin main
            aws ecr get-login-password --region us-east-1 | \
              docker login --username AWS --password-stdin ${{ env.ECR_REGISTRY }}
            docker-compose -f deployments/docker-compose.yml \
              -f deployments/docker-compose.freetier.yml pull
            docker-compose -f deployments/docker-compose.yml \
              -f deployments/docker-compose.freetier.yml up -d
            docker image prune -f
```

GitHub Actions secrets to configure:
- `AWS_ACCOUNT_ID`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `EC2_HOST` (Elastic IP)
- `EC2_SSH_KEY` (contents of the .pem file)

---

### Step 10: Monitoring

#### Free options:

1. **CloudWatch** (free tier: 10 custom metrics, 5 GB log ingestion, 3 dashboards)

```bash
# Install CloudWatch agent on EC2
sudo yum install -y amazon-cloudwatch-agent
```

2. **Prometheus + Grafana** are already in the Docker Compose stack. Access:
   - Prometheus: `http://<EC2_IP>:9090`
   - Grafana: `http://<EC2_IP>:3000` (admin/admin)

3. **Health check endpoint** — the API exposes a health endpoint. Set up a free uptime monitor via [UptimeRobot](https://uptimerobot.com) (50 monitors free).

---

### Free Tier Cost Breakdown

| Service | Free Tier Allowance | NotifyHub Usage | Monthly Cost |
|---------|-------------------|-----------------|-------------|
| EC2 t2.micro | 750 hrs/mo (12 months) | 1 instance, 24/7 | $0.00 |
| RDS db.t3.micro | 750 hrs/mo (12 months) | 1 instance | $0.00 |
| ECR | 500 MB (12 months) | ~100 MB (2 images) | $0.00 |
| EBS (EC2 storage) | 30 GB gp2 | 20 GB | $0.00 |
| RDS storage | 20 GB gp2 | 20 GB | $0.00 |
| SES (email) | 3,000 msgs/mo (from EC2) | Low volume | $0.00 |
| CloudWatch | 10 metrics, 5 GB logs | Basic monitoring | $0.00 |
| Elastic IP | Free when attached | 1 IP | $0.00 |
| Data Transfer | 100 GB/mo outbound | Low traffic | $0.00 |
| **Total** | | | **$0.00** |

> After 12 months, expect ~$15-25/mo for EC2 + RDS at the smallest instance sizes.

---

## Part 2: Production Deployment

This section describes a fully production-ready architecture. This is included for reference and future planning.

### Production Architecture Overview

```
                         ┌──────────────┐
                         │   Route 53   │
                         │ (DNS + Health│
                         │   Checks)    │
                         └──────┬───────┘
                                │
                         ┌──────▼───────┐
                         │  CloudFront  │
                         │    (CDN)     │
                         └──────┬───────┘
                                │
                         ┌──────▼───────┐
                         │     WAF      │
                         └──────┬───────┘
                                │
                    ┌───────────▼───────────┐
                    │    ALB (public)       │
                    │  SSL termination      │
                    └───────────┬───────────┘
                                │
                 ┌──────────────┼──────────────┐
                 │                             │
          ┌──────▼──────┐              ┌──────▼──────┐
          │  ECS API    │              │  ECS API    │
          │  (Fargate)  │              │  (Fargate)  │
          │  AZ-a       │              │  AZ-b       │
          └──────┬──────┘              └──────┬──────┘
                 │                             │
    ┌────────────┼────────────┬────────────────┤
    │            │            │                │
    ▼            ▼            ▼                ▼
┌───────┐  ┌────────┐  ┌──────────┐   ┌──────────┐
│Aurora  │  │Elasti- │  │  MSK     │   │ECS Worker│
│PG     │  │Cache   │  │ (Kafka)  │   │(Fargate) │
│Multi- │  │Redis   │  │ 3-broker │   │ Auto-    │
│AZ     │  │Cluster │  │ cluster  │   │ scaling  │
└───────┘  └────────┘  └──────────┘   └──────────┘

     Private Subnets (no public IPs)
     ────────────────────────────────
     VPC with NAT Gateway for outbound
```

---

### Infrastructure as Code

Use **Terraform** to manage all AWS resources. Suggested module layout:

```
terraform/
├── environments/
│   ├── staging/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── terraform.tfvars
│   └── production/
│       ├── main.tf
│       ├── variables.tf
│       └── terraform.tfvars
├── modules/
│   ├── vpc/
│   ├── ecs/
│   ├── rds/
│   ├── elasticache/
│   ├── msk/
│   ├── alb/
│   ├── ecr/
│   └── monitoring/
└── backend.tf          # S3 + DynamoDB state locking
```

State management:

```hcl
terraform {
  backend "s3" {
    bucket         = "notifyhub-terraform-state"
    key            = "production/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}
```

---

### Compute Layer

#### ECS Fargate (recommended)

```hcl
# API service
resource "aws_ecs_service" "api" {
  name            = "notifyhub-api"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = 2  # minimum for HA
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnets
    security_groups  = [aws_security_group.api.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "api"
    container_port   = 8080
  }
}

# API task definition
resource "aws_ecs_task_definition" "api" {
  family                   = "notifyhub-api"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512   # 0.5 vCPU
  memory                   = 1024  # 1 GB

  container_definitions = jsonencode([{
    name  = "api"
    image = "${aws_ecr_repository.api.repository_url}:latest"
    portMappings = [{
      containerPort = 8080
      protocol      = "tcp"
    }]
    environment = [
      { name = "PORT", value = "8080" },
      { name = "ENVIRONMENT", value = "production" },
      { name = "LOG_LEVEL", value = "info" },
    ]
    secrets = [
      { name = "DATABASE_URL", valueFrom = aws_ssm_parameter.db_url.arn },
      { name = "REDIS_URL", valueFrom = aws_ssm_parameter.redis_url.arn },
      { name = "KAFKA_BROKERS", valueFrom = aws_ssm_parameter.kafka_brokers.arn },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/notifyhub-api"
        "awslogs-region"        = "us-east-1"
        "awslogs-stream-prefix" = "api"
      }
    }
  }])
}

# Worker auto-scaling based on Kafka consumer lag
resource "aws_appautoscaling_target" "worker" {
  max_capacity       = 10
  min_capacity       = 2
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.worker.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "worker_cpu" {
  name               = "worker-cpu-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.worker.resource_id
  scalable_dimension = aws_appautoscaling_target.worker.scalable_dimension
  service_namespace  = aws_appautoscaling_target.worker.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value = 70.0
  }
}
```

#### Resource Sizing (Production)

| Service | Instance/Size | Count | Purpose |
|---------|-------------|-------|---------|
| ECS API | 0.5 vCPU / 1 GB | 2-4 | HTTP request handling |
| ECS Worker | 1 vCPU / 2 GB | 2-10 | Notification processing |
| Aurora PG | db.r6g.large | 2 (writer + reader) | Primary data store |
| ElastiCache | cache.r6g.large | 2 (primary + replica) | Rate limiting, caching |
| MSK | kafka.m5.large | 3 | Event streaming |

---

### Data Layer

#### Aurora PostgreSQL (Multi-AZ)

```hcl
resource "aws_rds_cluster" "main" {
  cluster_identifier     = "notifyhub"
  engine                 = "aurora-postgresql"
  engine_version         = "16.1"
  database_name          = "notifyhub"
  master_username        = "notifyhub"
  master_password        = var.db_password  # Use AWS Secrets Manager in practice
  storage_encrypted      = true
  deletion_protection    = true
  backup_retention_period = 14
  preferred_backup_window = "03:00-04:00"
  vpc_security_group_ids = [aws_security_group.db.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name

  serverlessv2_scaling_configuration {
    min_capacity = 0.5
    max_capacity = 4.0
  }
}

resource "aws_rds_cluster_instance" "writer" {
  cluster_identifier = aws_rds_cluster.main.id
  instance_class     = "db.serverless"
  engine             = "aurora-postgresql"
}

resource "aws_rds_cluster_instance" "reader" {
  cluster_identifier = aws_rds_cluster.main.id
  instance_class     = "db.serverless"
  engine             = "aurora-postgresql"
}
```

#### ElastiCache Redis (Cluster Mode)

```hcl
resource "aws_elasticache_replication_group" "main" {
  replication_group_id = "notifyhub-redis"
  description          = "NotifyHub Redis cluster"
  node_type            = "cache.r6g.large"
  num_cache_clusters   = 2
  engine_version       = "7.0"
  port                 = 6379
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  automatic_failover_enabled = true
  subnet_group_name    = aws_elasticache_subnet_group.main.name
  security_group_ids   = [aws_security_group.redis.id]
}
```

#### Amazon MSK (Managed Kafka)

```hcl
resource "aws_msk_cluster" "main" {
  cluster_name           = "notifyhub"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = var.private_subnets
    security_groups = [aws_security_group.kafka.id]

    storage_info {
      ebs_storage_info {
        volume_size = 100
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = "/msk/notifyhub"
      }
    }
  }
}
```

---

### Networking & Security

#### VPC Layout

```
VPC: 10.0.0.0/16
├── Public Subnets (ALB, NAT Gateway)
│   ├── 10.0.1.0/24 (AZ-a)
│   └── 10.0.2.0/24 (AZ-b)
├── Private App Subnets (ECS tasks)
│   ├── 10.0.10.0/24 (AZ-a)
│   └── 10.0.11.0/24 (AZ-b)
└── Private Data Subnets (RDS, ElastiCache, MSK)
    ├── 10.0.20.0/24 (AZ-a)
    └── 10.0.21.0/24 (AZ-b)
```

#### Security Groups

| Resource | Inbound | Source |
|----------|---------|--------|
| ALB | 443 (HTTPS) | 0.0.0.0/0 |
| API ECS | 8080 | ALB SG |
| Worker ECS | — | — (outbound only) |
| RDS | 5432 | App Subnet SG |
| Redis | 6379 | App Subnet SG |
| MSK | 9094 (TLS) | App Subnet SG |

#### Secrets Management

Store all sensitive configuration in AWS Systems Manager Parameter Store or Secrets Manager:

```bash
# Database URL
aws ssm put-parameter --name "/notifyhub/prod/DATABASE_URL" \
  --type "SecureString" \
  --value "postgres://..."

# API keys
aws ssm put-parameter --name "/notifyhub/prod/SES_API_KEY" \
  --type "SecureString" \
  --value "..."
```

---

### Observability Stack

#### Logging

- **CloudWatch Logs** for all ECS tasks (auto-configured via `awslogs` driver)
- Log retention: 30 days for production, 7 days for staging
- Structured JSON logs (slog already outputs JSON)

#### Metrics

- **CloudWatch Container Insights** for ECS metrics (CPU, memory, network)
- **Prometheus** (self-hosted or Amazon Managed Prometheus) for application metrics
- **Grafana** (self-hosted or Amazon Managed Grafana) for dashboards

#### Tracing

- **AWS X-Ray** or **OpenTelemetry Collector** -> Jaeger/Tempo
- Already instrumented via OpenTelemetry in the application

#### Alerting

```hcl
# High error rate alert
resource "aws_cloudwatch_metric_alarm" "api_5xx" {
  alarm_name          = "notifyhub-api-5xx-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "HTTPCode_Target_5XX_Count"
  namespace           = "AWS/ApplicationELB"
  period              = 300
  statistic           = "Sum"
  threshold           = 10
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# Kafka consumer lag alert
resource "aws_cloudwatch_metric_alarm" "consumer_lag" {
  alarm_name          = "notifyhub-kafka-lag-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "SumOffsetLag"
  namespace           = "AWS/Kafka"
  period              = 300
  statistic           = "Maximum"
  threshold           = 10000
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
```

---

### Production CI/CD Pipeline

```
┌─────────┐    ┌──────┐    ┌──────────┐    ┌─────────┐    ┌────────────┐
│  Push   │───>│ Test │───>│  Build   │───>│ Deploy  │───>│  Deploy    │
│  to PR  │    │ Lint │    │  Images  │    │ Staging │    │ Production │
└─────────┘    └──────┘    └──────────┘    └─────────┘    └────────────┘
                                                │               │
                                           Auto deploy    Manual approval
                                                          + canary rollout
```

Key practices:

1. **Blue/Green deployments** via ECS deployment circuit breaker
2. **Database migrations** run as a separate ECS task before deployment
3. **Canary releases** — route 10% traffic to new version, monitor, then full rollout
4. **Rollback** — ECS automatically rolls back if health checks fail

```yaml
# Production deploy job (GitHub Actions)
deploy-production:
  needs: deploy-staging
  runs-on: ubuntu-latest
  environment:
    name: production    # Requires manual approval in GitHub
  steps:
    - name: Run migrations
      run: |
        aws ecs run-task \
          --cluster notifyhub-prod \
          --task-definition notifyhub-migrate \
          --launch-type FARGATE \
          --network-configuration "..." \
          --overrides '{"containerOverrides":[{"name":"migrate","command":["up"]}]}'

    - name: Deploy API (rolling update)
      run: |
        aws ecs update-service \
          --cluster notifyhub-prod \
          --service notifyhub-api \
          --force-new-deployment \
          --deployment-configuration "maximumPercent=200,minimumHealthyPercent=100"

    - name: Deploy Worker
      run: |
        aws ecs update-service \
          --cluster notifyhub-prod \
          --service notifyhub-worker \
          --force-new-deployment
```

---

### Disaster Recovery

| Strategy | RPO | RTO | Implementation |
|----------|-----|-----|---------------|
| **Database** | ~1 sec | < 1 min | Aurora Multi-AZ with auto-failover |
| **Redis** | ~seconds | < 1 min | ElastiCache Multi-AZ with auto-failover |
| **Kafka** | 0 (replicated) | < 5 min | MSK 3-broker cluster across AZs |
| **Application** | N/A | < 5 min | ECS auto-restarts failed tasks |
| **Cross-Region** | < 1 hour | < 30 min | Aurora Global Database + Route 53 failover |

Backup strategy:
- **RDS**: Automated daily snapshots, 14-day retention, copy to another region weekly
- **Kafka**: Topic data retained for 7 days (configurable), critical topics replicated
- **Application state**: Stateless — no backup needed, just redeploy

---

### Production Cost Estimate

| Service | Spec | Monthly Cost |
|---------|------|-------------|
| ECS Fargate (API, 2 tasks) | 0.5 vCPU / 1 GB each | ~$30 |
| ECS Fargate (Worker, 2 tasks) | 1 vCPU / 2 GB each | ~$60 |
| Aurora PostgreSQL Serverless v2 | 0.5-4 ACU, 50 GB | ~$80 |
| ElastiCache Redis | cache.r6g.large x2 | ~$300 |
| MSK (3 brokers) | kafka.m5.large, 100 GB | ~$650 |
| ALB | 1 ALB + traffic | ~$25 |
| NAT Gateway | 2 (one per AZ) | ~$65 |
| CloudWatch | Logs + metrics | ~$30 |
| ECR | Image storage | ~$5 |
| SES | Email delivery | ~$10 |
| Data Transfer | Outbound | ~$20 |
| **Total** | | **~$1,275/mo** |

Cost optimization options:
- Use **Fargate Spot** for workers (up to 70% savings)
- Use **Aurora Serverless v2** to scale to zero during low traffic
- Use **Reserved Instances** for predictable workloads (up to 40% savings)
- Replace MSK with **SQS + SNS** if strict ordering isn't needed (~$5/mo vs $650)
- Use a single NAT Gateway (~$32/mo savings, less HA)
