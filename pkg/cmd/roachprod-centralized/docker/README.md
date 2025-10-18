# roachprod-centralized Docker Deployment

This directory contains Docker configuration for containerized deployment of the roachprod-centralized service.

**📚 Related Documentation:**
- [← Back to Main README](../README.md)
- [🔌 API Reference](../docs/API.md) - Complete REST API documentation
- [🏗️ Architecture Guide](../docs/ARCHITECTURE.md) - System design and components
- [💻 Development Guide](../docs/DEVELOPMENT.md) - Local development setup
- [📋 Examples & Workflows](../docs/EXAMPLES.md) - Practical usage examples
- [⚙️ Configuration Examples](../examples/) - Ready-to-use configurations

## Quick Start

### Build the Docker Image

```bash
# Use the build script
./push.sh
```

### Run the Container

```bash
# Basic run with minimal configuration
docker run -p 8080:8080 -p 8081:8081 \
  -e ROACHPROD_API_AUTHENTICATION_DISABLED=true \
  -e ROACHPROD_DATABASE_TYPE=memory \
  roachprod-centralized api

# With custom configuration file
docker run -p 8080:8080 -p 8081:8081 \
  -v /path/to/config.yaml:/etc/roachprod/config.yaml \
  roachprod-centralized api --config /etc/roachprod/config.yaml
```

## Docker Compose Example

Create a `docker-compose.yml` for local development:

```yaml
version: '3.8'
services:
  roachprod-centralized:
    build: .
    ports:
      - "8080:8080"  # API port
      - "8081:8081"  # Metrics port
    environment:
      # Development configuration
      - ROACHPROD_LOG_LEVEL=debug
      - ROACHPROD_API_AUTHENTICATION_DISABLED=true
      - ROACHPROD_DATABASE_TYPE=memory
      - ROACHPROD_TASKS_WORKERS=2
    volumes:
      # Mount cloud provider credentials
      - ./secrets:/secrets:ro
      # Mount custom configuration
      - ./config.yaml:/etc/roachprod/config.yaml:ro
    command: ["api", "--config", "/etc/roachprod/config.yaml"]

  # Optional: Add CockroachDB for persistence
  cockroachdb:
    image: cockroachdb/cockroach:latest
    command: start-single-node --insecure --http-addr=0.0.0.0:8080
    ports:
      - "26257:26257"
      - "8090:8080"  # CockroachDB Admin UI
    volumes:
      - cockroach-data:/cockroach/cockroach-data

volumes:
  cockroach-data:
```

Run with: `docker-compose up`

## Environment Variables

The container supports all roachprod-centralized configuration via environment variables:

### Core Configuration

```bash
# API Configuration
ROACHPROD_API_PORT=8080                              # API server port
ROACHPROD_API_BASE_PATH=""                          # Base URL path
ROACHPROD_API_METRICS_ENABLED=true                 # Enable metrics
ROACHPROD_API_METRICS_PORT=8081                    # Metrics port

# Authentication
ROACHPROD_API_AUTHENTICATION_DISABLED=true         # Disable auth (dev only)
ROACHPROD_API_AUTHENTICATION_JWT_HEADER="X-Goog-IAP-JWT-Assertion"
ROACHPROD_API_AUTHENTICATION_JWT_AUDIENCE=""       # JWT audience

# Logging
ROACHPROD_LOG_LEVEL=info                           # debug|info|warn|error
```

### Database Configuration

```bash
# In-memory (development)
ROACHPROD_DATABASE_TYPE=memory

# CockroachDB (production)
ROACHPROD_DATABASE_TYPE=cockroachdb
ROACHPROD_DATABASE_URL="postgresql://user:password@cockroachdb:26257/roachprod?sslmode=require"
ROACHPROD_DATABASE_MAX_CONNS=10
ROACHPROD_DATABASE_MAX_IDLE_TIME=300
```

### Task Processing

```bash
ROACHPROD_TASKS_WORKERS=3                          # Number of background workers
```

### Cloud Provider Configuration

Mount cloud provider credentials and configuration:

```bash
# Google Cloud Platform
GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp-key.json

# AWS
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
# Or mount ~/.aws/credentials to /secrets/aws-credentials

# Azure
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
AZURE_TENANT_ID=your-tenant-id

# IBM Cloud
IC_API_KEY=your-api-key
```
### Health Checks

The container includes health check endpoints:

```bash
# Basic health check
curl http://localhost:8080/health

# Detailed health status
curl http://localhost:8080/health/detailed

# Prometheus metrics
curl http://localhost:8081/metrics
```

## Build and Deployment Scripts

### build.sh

Builds the Docker image locally:

```bash
./build.sh
```

### push.sh

Builds and pushes the image to a registry (for CI/CD):

```bash
# Push to default registry
./push.sh

# Push to custom registry with different owner
OWNER=myorg REPO=myrepo ./push.sh
```

**Note**: Ensure `kubectl` is available when running `./push.sh` as it may update Kubernetes resources.

## Dockerfile Structure

The Dockerfile:

1. **Base Image**: Uses a minimal base image with necessary CLI tools
2. **roachprod-centralized Binary**: Copies the pre-built binary
3. **Entrypoint**: Configures CLI tools using mounted secrets
4. **Security**: Runs as non-root user
5. **Health Checks**: Includes built-in health check support

## Secrets and Configuration

### Secrets Directory Structure

Mount secrets to `/secrets` with this structure:

```
/secrets/
├── gcp-key.json              # GCP service account key
├── aws-credentials           # AWS credentials file
├── azure-credentials         # Azure service principal config
└── cloud-config.yaml         # Custom cloud provider config
```

### Security Considerations

1. **Never include credentials in the image**
2. **Use Kubernetes secrets** for sensitive data
3. **Enable authentication** in production environments
4. **Use read-only volume mounts** for secrets
5. **Regularly rotate credentials**

## Monitoring

### Prometheus Integration

The container exposes Prometheus metrics on port 8081:

```yaml
# Prometheus scrape configuration
- job_name: 'roachprod-centralized'
  static_configs:
  - targets: ['roachprod-centralized:8081']
  metrics_path: '/metrics'
```

### Logging

Structured JSON logs are written to stdout/stderr:

```json
{
  "level": "info",
  "time": "2025-01-15T10:30:00Z",
  "msg": "Starting roachprod-centralized API server",
  "port": 8080
}
```

Configure log aggregation to collect these logs in production environments.

## Troubleshooting

### Common Issues

**Container won't start:**
```bash
# Check container logs
docker logs <container-id>

# Verify environment variables
docker exec <container-id> env | grep ROACHPROD
```

**Authentication failures:**
```bash
# For development, disable authentication
docker run -e ROACHPROD_API_AUTHENTICATION_DISABLED=true ...
```

**Database connection issues:**
```bash
# Use in-memory database for testing
docker run -e ROACHPROD_DATABASE_TYPE=memory ...
```

**Port conflicts:**
```bash
# Use different ports
docker run -p 9090:8080 -p 9091:8081 ...
```
