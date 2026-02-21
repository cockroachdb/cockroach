# Cloud Provider Configuration for roachprod-centralized

This document describes how to configure cloud providers for roachprod-centralized.

## Configuration Methods

Cloud providers can be configured in three ways (in order of precedence):

1. Using environment variables with the `ROACHPROD_` prefix (highest precedence)
   - Modern format: `ROACHPROD_PROVIDERS_<index>_<property>` (e.g., `ROACHPROD_PROVIDERS_0_GCE_PROJECT`)
   - Legacy format: `ROACHPROD_<provider>_<property>_<index>` (e.g., `ROACHPROD_GCE_PROJECT_0`)
2. Using a YAML configuration file specified by `CloudConfigPath` option
3. Using a YAML configuration file specified by the `ROACHPROD_CLOUD_CONFIG` environment variable
4. Using the default built-in configuration (lowest precedence)

**Note**: Environment variables take precedence over file configurations. If a provider is defined in both places, the environment variable configuration will be used.

## YAML Configuration File Format

The YAML configuration file follows this structure:

```yaml
providers:
  # GCE Provider
  - type: gce
    environment: gcp-engineering  # authorization scope for permission checks
    gce:
      project: project-id
      metadata_project: metadata-project-id
      dns_project: dns-project-id
      dns_public_zone: public-zone
      dns_public_domain: public-domain
      dns_managed_zone: managed-zone
      dns_managed_domain: managed-domain

  # AWS Provider
  - type: aws
    environment: aws-staging  # authorization scope for permission checks
    aws:
      account_id: "account-id"
      assume_sts_role: true

  # Azure Provider
  - type: azure
    environment: azure-production  # authorization scope for permission checks
    azure:
      subscription_name: subscription-name

  # IBM Provider
  - type: ibm
    environment: ibm-default  # authorization scope for permission checks
```

### Common Provider Fields

Each provider entry supports these common fields:

| Field | Required | Description |
|-------|----------|-------------|
| `type` | Yes | Cloud provider type: `gce`, `aws`, `azure`, or `ibm` |
| `environment` | Yes | Maps the provider to an authorization scope used for permission checks. Users are granted access to specific environments, and operations on a provider are authorized against its environment value. Choose a name that reflects the provider's purpose (e.g., `ephemeral`, `drt`, `aws-default`, `azure-infra`). |

See the example configuration file at `pkg/cmd/roachprod-centralized/examples/cloud_config.yaml.example`.

## Environment Variables Configuration

There are two supported formats for environment variable configuration:

### 1. Direct Provider Configuration Pattern (Recommended)

This pattern allows you to directly override fields in the YAML configuration or add new providers:

```
ROACHPROD_PROVIDERS_<index>_<field>=value
```

Where:
- `<index>` is the zero-based index of the provider in the configuration
- `<field>` is the field path in the configuration

#### Examples:

Override the project of the first GCE provider:
```
ROACHPROD_PROVIDERS_0_GCE_PROJECT=my-custom-project
```

Set the environment (authorization scope) for a provider:
```
ROACHPROD_PROVIDERS_0_ENVIRONMENT=ephemeral
```

Add a new Azure provider:
```
ROACHPROD_PROVIDERS_9_TYPE=azure
ROACHPROD_PROVIDERS_9_ENVIRONMENT=azure-staging
ROACHPROD_PROVIDERS_9_AZURE_SUBSCRIPTION_NAME=my-subscription
```

Change the account ID of an existing AWS provider (at index 3):
```
ROACHPROD_PROVIDERS_3_AWS_ACCOUNT_ID=123456789
```

Add a simple IBM provider:
```
ROACHPROD_PROVIDERS_5_TYPE=ibm
ROACHPROD_PROVIDERS_5_IBM=true
```

### 2. Legacy Provider Type Pattern

This older pattern is still supported for backward compatibility:

#### GCE Providers
```
ROACHPROD_GCE_PROJECT_0=project-id
ROACHPROD_GCE_METADATA_PROJECT_0=metadata-project-id
ROACHPROD_GCE_DNS_PROJECT_0=dns-project-id
ROACHPROD_GCE_DNS_PUBLIC_ZONE_0=public-zone
ROACHPROD_GCE_DNS_PUBLIC_DOMAIN_0=public-domain
ROACHPROD_GCE_DNS_MANAGED_ZONE_0=managed-zone
ROACHPROD_GCE_DNS_MANAGED_DOMAIN_0=managed-domain

# Additional GCE providers can be added with incrementing indices
ROACHPROD_GCE_PROJECT_1=another-project-id
```

#### AWS Providers
```
ROACHPROD_AWS_ACCOUNT_ID_0=account-id
ROACHPROD_AWS_ASSUME_STS_ROLE_0=true
```

#### Azure Providers
```
ROACHPROD_AZURE_SUBSCRIPTION_0=subscription-name
```

#### IBM Provider
```
ROACHPROD_IBM_ENABLED=true
```

## Usage in Code

To use the cloud provider configuration in your code:

```go
import (
    "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config"
)

// The simplest way: get cloud providers with automatic merging of configuration sources
providers, err := config.GetProviders("/path/to/config.yaml") // Path is optional
if err != nil {
    // Handle error
}

// Or if you need the configuration itself:
cloudConfig, err := config.GetProvidersConfig("/path/to/config.yaml") // Path is optional
if err != nil {
    // Handle error
}

// For more control, you can separately load configs from file or environment
fileConfig, err := config.LoadCloudConfigFromFile("/path/to/config.yaml")
envConfig, err := config.LoadCloudConfigFromEnv("ROACHPROD_")
```

## Default Configuration Path

If not specified, the configuration file is searched in the following locations:

1. Path provided to `GetCloudProviderConfigPath()`
2. Path specified by `ROACHPROD_CLOUD_CONFIG` environment variable
3. `~/.roachprod/cloud_config.yaml`
