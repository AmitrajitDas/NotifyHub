# Kubernetes Learning Guide — NotifyHub Context

Concepts to relearn before implementing `charts/notifyhub/`. Each section maps directly to something this project needs.

---

## 1. Core Objects (Must)

### Pod
Smallest deployable unit. One or more containers sharing network + storage.
- You won't create Pods directly — Deployments manage them.
- Each `api` / `worker` replica = one Pod.

### Deployment
Declares desired state: image, replicas, resource limits, env vars.
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  replicas: 2
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      containers:
        - name: api
          image: ghcr.io/amitrajitdas31/notifyhub-api:latest
          ports:
            - containerPort: 8080
```

### Service
Stable network endpoint in front of Pods (Pods are ephemeral, IPs change).
- `ClusterIP` — internal only (worker → api)
- `LoadBalancer` — external IP (cloud only)
- `NodePort` — maps to host port (kind/local)

### ConfigMap
Non-secret config as key-value. Replaces docker-compose `environment:` for non-sensitive values.
```yaml
# KAFKA_BROKERS, LOG_LEVEL, ENVIRONMENT, etc.
```

### Secret
Same as ConfigMap but base64-encoded. For `DATABASE_URL`, `REDIS_URL`, provider creds.
```yaml
# Never commit real secrets. Use sealed-secrets or external-secrets in prod.
```

---

## 2. Health Probes (Must)

Your code already exposes `/livez` and `/readyz`. Wire them up:

```yaml
livenessProbe:
  httpGet:
    path: /livez
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 15

readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

- **Liveness** — is the process alive? Fail = restart container.
- **Readiness** — is it ready to serve traffic? Fail = remove from Service load balancer.

---

## 3. kubectl Basics (Must)

```bash
kubectl apply -f manifest.yaml          # create/update resources
kubectl get pods -n notifyhub           # list pods
kubectl describe pod <name> -n notifyhub  # debug events + state
kubectl logs <pod> -n notifyhub -f      # stream logs
kubectl exec -it <pod> -n notifyhub -- sh  # shell into container
kubectl delete -f manifest.yaml         # remove resources
kubectl get events -n notifyhub --sort-by='.lastTimestamp'  # debug failures
```

---

## 4. Helm (Must)

Package manager for k8s. A **chart** = templated k8s manifests + `values.yaml`.

### Chart structure
```
charts/notifyhub/
  Chart.yaml          # name, version, description
  values.yaml         # default values (image tag, replicas, resources)
  values.kind.yaml    # local overrides (lower resources, NodePort)
  values.eks.yaml     # EKS overrides (LoadBalancer, higher resources)
  templates/
    _helpers.tpl      # reusable template functions (labels, names)
    api/
      deployment.yaml
      service.yaml
      hpa.yaml
      ingress.yaml
    worker/
      deployment.yaml
      hpa.yaml
    configmap.yaml
    secret.yaml
    serviceaccount.yaml
```

### Key commands
```bash
helm install notifyhub ./charts/notifyhub -f values.kind.yaml -n notifyhub --create-namespace
helm upgrade notifyhub ./charts/notifyhub -f values.kind.yaml -n notifyhub
helm uninstall notifyhub -n notifyhub
helm template ./charts/notifyhub -f values.kind.yaml  # render without applying (debug)
helm lint ./charts/notifyhub                           # validate
```

### Templating basics
```yaml
# templates/api/deployment.yaml
name: {{ include "notifyhub.fullname" . }}-api
image: "{{ .Values.api.image.repository }}:{{ .Values.api.image.tag }}"
replicas: {{ .Values.api.replicaCount }}
```

---

## 5. HPA — Horizontal Pod Autoscaler (Should)

Scales replicas based on CPU/memory (or custom metrics via Prometheus adapter).

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: worker
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: worker
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

Interview story: worker CPU spikes during Kafka lag bursts → HPA adds replicas → lag clears.

---

## 6. Ingress (Should)

Routes external HTTP traffic to Services. Needs an **Ingress Controller** (nginx is standard).

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: api
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
spec:
  ingressClassName: nginx
  rules:
    - host: notifyhub.local       # kind local
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: api
                port:
                  number: 8080
```

Install nginx controller in kind:
```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
```

---

## 7. kind — Local Cluster (Should)

Kubernetes in Docker. Free, fast, EKS-compatible configs.

```bash
# Install
brew install kind

# Create cluster
kind create cluster --config deploy/kind-config.yaml

# Load local image (no registry needed)
kind load docker-image ghcr.io/amitrajitdas31/notifyhub-api:dev

# Delete cluster
kind delete cluster
```

`deploy/kind-config.yaml` maps host ports so Ingress works locally:
```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
    extraPortMappings:
      - containerPort: 80
        hostPort: 80
        protocol: TCP
      - containerPort: 443
        hostPort: 443
        protocol: TCP
```

---

## 8. Namespaces + RBAC (Nice to have)

```bash
kubectl create namespace notifyhub
```

ServiceAccount per app → least-privilege. Not critical for local, required for EKS with IAM roles.

---

## Learning Order

1. `kubectl` + kind cluster → get a pod running locally
2. Write a Deployment + Service manually → understand the YAML shape
3. Add ConfigMap + Secret → replace hardcoded env vars
4. Add health probes → watch pod restart behavior
5. Install nginx ingress → hit the api from localhost
6. Add HPA → watch `kubectl get hpa -w`
7. Learn Helm → convert manual YAMLs into a chart
8. Add `values.kind.yaml` overrides → understand value hierarchy

## Resources

- [Kubernetes docs — Concepts](https://kubernetes.io/docs/concepts/)
- [Helm docs](https://helm.sh/docs/)
- [kind quickstart](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [kubectl cheatsheet](https://kubernetes.io/docs/reference/kubectl/cheatsheet/)
