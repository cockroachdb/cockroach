// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/ibm"
	"github.com/iancoleman/strcase"
	"gopkg.in/yaml.v3"
)

const (
	// EnvPrefix is the prefix used for environment variables.
	EnvPrefix        = "ROACHPROD"
	MetricsNamespace = "roachprod"
)

// Config holds the configuration for the application.
type Config struct {
	Config string `env:"CONFIG" default:"" description:"Path to configuration file"`
	Log    struct {
		Level string `env:"LEVEL" default:"info" description:"Logging level (debug, info, warn, error)"`
	} `env:"LOG" description:"Logging configuration"`
	Api struct {
		Port     int    `env:"PORT" default:"8080" description:"HTTP API port"`
		BasePath string `env:"BASE_PATH" default:"" description:"Base URL for API endpoints"`
		Metrics  struct {
			Enabled bool   `env:"ENABLED" default:"true" description:"Enable metrics"`
			Path    string `env:"PATH" default:"/metrics" description:"Metrics endpoint path"`
			Port    int    `env:"PORT" default:"8081" description:"Metrics HTTP port"`
		} `env:"METRICS" description:"Metrics configuration"`
		TrustedProxies []string `env:"TRUSTED_PROXIES" description:"Trusted proxy CIDRs for X-Forwarded-For parsing (empty = trust no proxies, use RemoteAddr)"`
		Authentication struct {
			Type   string `env:"TYPE" default:"jwt" description:"Authentication type (disabled, jwt, bearer)"`
			Header string `env:"HEADER" default:"Authorization" description:"HTTP header for authentication token"`
			JWT    struct {
				Audience string `env:"AUDIENCE" default:"" description:"JWT audience for authentication"`
				Issuer   string `env:"ISSUER" default:"https://cloud.google.com/iap" description:"JWT issuer for authentication"`
			} `env:"JWT" description:"JWT authentication configuration"`
			Bearer struct {
				OktaIssuer   string `env:"OKTA_ISSUER" default:"" description:"Okta issuer URL (e.g., https://dev-123456.okta.com/oauth2/default)"`
				OktaAudience string `env:"OKTA_AUDIENCE" default:"" description:"Okta audience for token validation"`
			} `env:"BEARER" description:"Bearer token authentication configuration"`
		} `env:"AUTHENTICATION" description:"API authentication configuration"`
	} `env:"API" description:"API configuration"`
	InstanceHealthTimeoutSeconds int `env:"INSTANCE_HEALTH_TIMEOUT_SECONDS" default:"3" description:"Timeout in seconds to consider an instance healthy"`
	Tasks                        struct {
		Workers int `env:"WORKERS" default:"1" description:"Number of workers processing tasks"`
	} `env:"TASKS" description:"Tasks configuration"`
	Database struct {
		URL         string `env:"URL" default:"" description:"Database connection URL"`
		Type        string `env:"TYPE" default:"memory" description:"Database type (memory, cockroachdb)"`
		MaxConns    int    `env:"MAX_CONNS" default:"10" description:"Maximum database connections"`
		MaxIdleTime int    `env:"MAX_IDLE_TIME" default:"300" description:"Maximum idle time for connections in seconds"`
	} `env:"DATABASE" description:"Database configuration"`
	CloudProviders []CloudProvider `env:"CLOUDPROVIDERS" description:"List of cloud providers"`
	DNSProviders   []DNSProvider   `env:"DNSPROVIDERS" description:"List of DNS providers"`
	Bootstrap      struct {
		SCIMToken string `env:"SCIM_TOKEN" default:"" description:"Bootstrap SCIM service account token (used on first startup when no service accounts exist)"`
	} `env:"BOOTSTRAP" description:"Bootstrap configuration for initial setup"`
}

// CloudProvider represents a cloud provider configuration.
type CloudProvider struct {
	Type          string                `env:"TYPE" default:"" description:"Cloud provider type (aws, azure, gce, ibm)"`
	Environment   string                `env:"ENVIRONMENT" default:"" description:"Environment name for authorization scope (e.g., gcp-engineering, aws-staging)"`
	AWS           aws.ProviderOptions   `env:"AWS" description:"AWS cloud provider options"`
	Azure         azure.ProviderOptions `env:"AZURE" description:"Azure cloud provider options"`
	GCE           gce.ProviderOptions   `env:"GCE" description:"GCE cloud provider options"`
	IBM           ibm.ProviderOptions   `env:"IBM" description:"IBM cloud provider options"`
	DNSPublicZone string                `env:"DNSPUBLICZONE" description:"DNS zone for the cloud provider"`
}

type DNSProvider struct {
	Type   string                 `env:"TYPE" default:"" description:"DNS provider type (only 'gce' is supported for now)"`
	GCE    gce.DNSProviderOpts    `env:"GCE" description:"GCE DNS provider options"`
	GCESDK gce.SDKDNSProviderOpts `env:"GCESDK" description:"GCE SDK DNS provider options"`
}

// String returns a YAML representation of the config.
func (c *Config) String() string {
	o, _ := yaml.Marshal(c)
	return string(o)
}

// SliceOverride represents an override for a specific field in a slice element
type SliceOverride struct {
	SliceField string      // "CloudProviders" or "DNSProviders"
	Index      int         // 0, 1, 2, etc.
	FieldPath  string      // "Type", "AWS.Region", "GCE.Project", etc.
	Value      interface{} // The override value
	Source     string      // "flag", "env"
}

// SliceIndexInfo holds information about discovered slice indices
type SliceIndexInfo struct {
	SliceField string
	MaxIndex   int
	Indices    []int
}

// GetSliceFieldNames dynamically discovers slice field names from Config struct using reflection
func GetSliceFieldNames() []string {
	envToField := GetSliceFieldMapping()
	var sliceFields []string
	for envName := range envToField {
		sliceFields = append(sliceFields, envName)
	}
	return sliceFields
}

// GetSliceFieldMapping creates a mapping from environment variable names to struct field names
func GetSliceFieldMapping() map[string]string {
	envToField := make(map[string]string)

	// Get the Config struct type
	configType := reflect.TypeOf(Config{})

	// Iterate through all fields in Config
	for i := range configType.NumField() {
		field := configType.Field(i)

		// Check if this field is a slice
		if field.Type.Kind() == reflect.Slice {
			// Use env tag if available, otherwise convert field name
			envTag := field.Tag.Get("env")
			if envTag == "" {
				envTag = strcase.ToSnake(field.Name)
			}
			envName := strings.ToUpper(envTag)
			envToField[envName] = field.Name
		}
	}

	return envToField
}

// FlagNameToSliceField converts kebab-case flag name to PascalCase field name
// cloud-providers -> CloudProviders, dns-providers -> DNSProviders
func FlagNameToSliceField(flagName string) string {
	return strcase.ToCamel(flagName)
}

// EnvNameToSliceField uses the mapping to get the correct field name
func EnvNameToSliceField(envName string) string {
	envToField := GetSliceFieldMapping()
	if fieldName, exists := envToField[envName]; exists {
		return fieldName
	}
	// Fallback to generic conversion if not found (shouldn't happen)
	return strcase.ToCamel(strings.ToLower(envName))
}

// FlagNameToFieldPath converts kebab-case to field path using generic conversion
func FlagNameToFieldPath(flagPath string) string {
	parts := strings.Split(flagPath, "-")

	// Handle nested field paths: provider-field -> Provider.Field
	if len(parts) >= 2 {
		// Check if first part looks like a provider name (all uppercase when converted)
		firstPart := strings.ToUpper(parts[0])
		// Convert the first part to title case for the provider field name
		providerName := strcase.ToCamel(parts[0])
		// Convert remaining parts to a camel case field name
		subField := strcase.ToCamel(strings.Join(parts[1:], "-"))

		// If the first part is all caps when converted, treat as nested field
		if strings.ToUpper(providerName) == firstPart {
			return fmt.Sprintf("%s.%s", strings.ToUpper(parts[0]), subField)
		} else {
			// Use standard nested format
			return fmt.Sprintf("%s.%s", providerName, subField)
		}
	}

	// Simple field: convert to PascalCase
	return strcase.ToCamel(flagPath)
}

// EnvNameToFieldPath converts UPPER_CASE to field path using generic conversion
func EnvNameToFieldPath(envPath string) string {
	parts := strings.Split(envPath, "_")

	// Handle nested field paths: PROVIDER_FIELD -> Provider.Field
	if len(parts) >= 2 {
		// First part as provider name (keep uppercase for known providers)
		provider := parts[0]
		// Convert remaining parts to camel case field name
		subField := strcase.ToCamel(strings.ToLower(strings.Join(parts[1:], "_")))
		return fmt.Sprintf("%s.%s", provider, subField)
	}

	// Simple field: convert to PascalCase
	return strcase.ToCamel(strings.ToLower(envPath))
}
