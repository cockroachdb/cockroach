# roachprod-centralized Configuration Examples

This directory contains practical configuration examples for different deployment scenarios.

**📚 Related Documentation:**
- [← Back to Main README](../README.md)
- [🔌 API Reference](../docs/API.md) - Complete REST API documentation
- [🏗️ Architecture Guide](../docs/ARCHITECTURE.md) - System design and components
- [💻 Development Guide](../docs/DEVELOPMENT.md) - Local development setup
- [📋 Examples & Workflows](../docs/EXAMPLES.md) - Practical usage examples

## Files Overview

| File | Description | Use Case |
|------|-------------|----------|
| `development-config.yaml` | Configuration with production examples | Development and production deployment |
| `cloud_config.yaml.example` | Cloud provider configuration | Multi-cloud setup |
| `docker-compose.yml` | Docker Compose setup | Local testing with CockroachDB |

## Quick Start

### Local Development

1. **Using memory storage** (fastest setup):
   ```bash
   # development-config.yaml already uses memory storage by default
   ./dev run roachprod-centralized api --config examples/development-config.yaml
   ```

2. **Using Docker Compose** (with CockroachDB):
   ```bash
   cd examples/
   docker-compose up
   ```

### Production Deployment

```bash
# Copy and edit development-config.yaml with production settings (see comments in file)
cp examples/development-config.yaml /etc/roachprod/config.yaml
# Edit: enable authentication, configure CockroachDB, set workers, etc.
./dev run roachprod-centralized api --config /etc/roachprod/config.yaml
```

## Configuration Customization

### Environment Variables

All configuration values can be overridden with environment variables:

```bash
# Override log level
export ROACHPROD_LOG_LEVEL=debug

# Override database configuration
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="postgresql://user:pass@host:26257/db?sslmode=require"

# Override API settings
export ROACHPROD_API_PORT=9090
export ROACHPROD_API_AUTHENTICATION_METHOD=disabled
```

### Cloud Provider Setup

1. **Copy the cloud config example**:
   ```bash
   cp cloud_config.yaml.example ~/.roachprod/cloud_config.yaml
   ```

2. **Edit with your credentials**:
   - Update project IDs for GCP
   - Update account IDs for AWS
   - Update subscription names for Azure
   - Configure DNS zones and domains

3. **Set environment variable**:
   ```bash
   export ROACHPROD_CLOUD_CONFIG=~/.roachprod/cloud_config.yaml
   ```

### Secrets Management

For production deployments, manage secrets securely:

```bash
# Create secrets directory
mkdir -p secrets/
echo "postgresql://..." > secrets/database-url
cp gcp-key.json secrets/
```

## Monitoring

The service exposes Prometheus metrics on port 8081 (default):

```bash
curl http://localhost:8081/metrics
```

Available metric types:
- HTTP request metrics (duration, status codes)
- Task processing metrics (queue size, processing time)
- Database connection metrics
- Custom business metrics

## Security Considerations

### Production Checklist

- [ ] Enable authentication (`authentication.disabled: false`)
- [ ] Configure JWT audience for your environment
- [ ] Use TLS for database connections (`sslmode=require`)
- [ ] Rotate cloud provider credentials regularly
- [ ] Store secrets securely (not in version control)
- [ ] Enable audit logging

### Development Security

- [ ] Use authentication.disabled: true only in development
- [ ] Never commit credentials to version control
- [ ] Use separate cloud projects for development
- [ ] Regularly update dependencies

## Troubleshooting

### Common Issues

1. **Port conflicts**:
   ```bash
   # Change API port
   export ROACHPROD_API_PORT=9090
   ```

2. **Database connection failures**:
   ```bash
   # Test with memory storage
   export ROACHPROD_DATABASE_TYPE=memory
   ```

3. **Authentication errors**:
   ```bash
   # Disable for development
   export ROACHPROD_API_AUTHENTICATION_METHOD=disabled
   ```

4. **Cloud provider configuration**:
   ```bash
   # Enable debug logging
   export ROACHPROD_LOG_LEVEL=debug
   # Check logs for detailed error messages
   ```

### Health Checks

```bash
# API health
curl http://localhost:8080/health

# Metrics endpoint
curl http://localhost:8081/metrics

# Database connectivity (if using CockroachDB)
cockroach sql --url="postgresql://..." --execute="SELECT 1;"
```

## Next Steps

1. Review the [API documentation](../docs/API.md) for endpoint details
2. Check the [Architecture guide](../docs/ARCHITECTURE.md) for system design
3. See the [Development guide](../docs/DEVELOPMENT.md) for contribution guidelines
4. Explore [Examples documentation](../docs/EXAMPLES.md) for detailed workflows

## Support

For additional help:
- Check the main [README](../README.md)
- Review troubleshooting in individual config files
- Consult CockroachDB documentation for database issues