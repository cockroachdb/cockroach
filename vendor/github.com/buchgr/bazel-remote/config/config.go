package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"

	yaml "gopkg.in/yaml.v2"
)

// S3CloudStorageConfig stores the configuration of an S3 API proxy backend.
type S3CloudStorageConfig struct {
	Endpoint        string `yaml:"endpoint"`
	Bucket          string `yaml:"bucket"`
	Prefix          string `yaml:"prefix"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	DisableSSL      bool   `yaml:"disable_ssl"`
	IAMRoleEndpoint string `yaml:"iam_role_endpoint"`
	Region          string `yaml:"region"`
	KeyVersion      int    `yaml:"key_version"`
}

// GoogleCloudStorageConfig stores the configuration of a GCS proxy backend.
type GoogleCloudStorageConfig struct {
	Bucket                string `yaml:"bucket"`
	UseDefaultCredentials bool   `yaml:"use_default_credentials"`
	JSONCredentialsFile   string `yaml:"json_credentials_file"`
}

// HTTPBackendConfig stores the configuration for a HTTP proxy backend.
type HTTPBackendConfig struct {
	BaseURL string `yaml:"url"`
}

// Config holds the top-level configuration for bazel-remote.
type Config struct {
	Host                        string                    `yaml:"host"`
	Port                        int                       `yaml:"port"`
	GRPCPort                    int                       `yaml:"grpc_port"`
	ProfileHost                 string                    `yaml:"profile_host"`
	ProfilePort                 int                       `yaml:"profile_port"`
	Dir                         string                    `yaml:"dir"`
	MaxSize                     int                       `yaml:"max_size"`
	HtpasswdFile                string                    `yaml:"htpasswd_file"`
	TLSCaFile                   string                    `yaml:"tls_ca_file"`
	TLSCertFile                 string                    `yaml:"tls_cert_file"`
	TLSKeyFile                  string                    `yaml:"tls_key_file"`
	S3CloudStorage              *S3CloudStorageConfig     `yaml:"s3_proxy,omitempty"`
	GoogleCloudStorage          *GoogleCloudStorageConfig `yaml:"gcs_proxy,omitempty"`
	HTTPBackend                 *HTTPBackendConfig        `yaml:"http_proxy,omitempty"`
	NumUploaders                int                       `yaml:"num_uploaders"`
	MaxQueuedUploads            int                       `yaml:"max_queued_uploads"`
	IdleTimeout                 time.Duration             `yaml:"idle_timeout"`
	DisableHTTPACValidation     bool                      `yaml:"disable_http_ac_validation"`
	DisableGRPCACDepsCheck      bool                      `yaml:"disable_grpc_ac_deps_check"`
	EnableACKeyInstanceMangling bool                      `yaml:"enable_ac_key_instance_mangling"`
	EnableEndpointMetrics       bool                      `yaml:"enable_endpoint_metrics"`
	MetricsDurationBuckets      []float64                 `yaml:"endpoint_metrics_duration_buckets"`
	ExperimentalRemoteAssetAPI  bool                      `yaml:"experimental_remote_asset_api"`
	HTTPReadTimeout             time.Duration             `yaml:"http_read_timeout"`
	HTTPWriteTimeout            time.Duration             `yaml:"http_write_timeout"`
}

var defaultDurationBuckets = []float64{.5, 1, 2.5, 5, 10, 20, 40, 80, 160, 320}

// New returns a validated Config with the specified values, and an error
// if there were any problems with the validation.
func New(dir string, maxSize int, host string, port int, grpcPort int,
	profileHost string, profilePort int,
	htpasswdFile string,
	maxQueuedUploads int,
	numUploaders int,
	tlsCaFile string,
	tlsCertFile string,
	tlsKeyFile string,
	idleTimeout time.Duration,
	s3 *S3CloudStorageConfig,
	disableHTTPACValidation bool,
	disableGRPCACDepsCheck bool,
	enableACKeyInstanceMangling bool,
	enableEndpointMetrics bool,
	experimentalRemoteAssetAPI bool,
	httpReadTimeout time.Duration,
	httpWriteTimeout time.Duration) (*Config, error) {
	c := Config{
		Host:                        host,
		Port:                        port,
		GRPCPort:                    grpcPort,
		ProfileHost:                 profileHost,
		ProfilePort:                 profilePort,
		Dir:                         dir,
		MaxSize:                     maxSize,
		HtpasswdFile:                htpasswdFile,
		MaxQueuedUploads:            maxQueuedUploads,
		NumUploaders:                numUploaders,
		TLSCaFile:                   tlsCaFile,
		TLSCertFile:                 tlsCertFile,
		TLSKeyFile:                  tlsKeyFile,
		S3CloudStorage:              s3,
		GoogleCloudStorage:          nil,
		HTTPBackend:                 nil,
		IdleTimeout:                 idleTimeout,
		DisableHTTPACValidation:     disableHTTPACValidation,
		DisableGRPCACDepsCheck:      disableGRPCACDepsCheck,
		EnableACKeyInstanceMangling: enableACKeyInstanceMangling,
		EnableEndpointMetrics:       enableEndpointMetrics,
		MetricsDurationBuckets:      defaultDurationBuckets,
		ExperimentalRemoteAssetAPI:  experimentalRemoteAssetAPI,
		HTTPReadTimeout:             httpReadTimeout,
		HTTPWriteTimeout:            httpWriteTimeout,
	}

	err := validateConfig(&c)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// NewFromYamlFile reads configuration settings from a YAML file then returns
// a validated Config with those settings, and an error if there were any
// problems.
func NewFromYamlFile(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to open config file '%s': %v", path, err)
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("Failed to read config file '%s': %v", path, err)
	}

	return newFromYaml(data)
}

func newFromYaml(data []byte) (*Config, error) {
	c := Config{
		NumUploaders:           100,
		MaxQueuedUploads:       1000000,
		MetricsDurationBuckets: defaultDurationBuckets,
	}

	err := yaml.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse YAML config: %v", err)
	}

	if c.MetricsDurationBuckets != nil {
		sort.Float64s(c.MetricsDurationBuckets)
	}

	err = validateConfig(&c)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func validateConfig(c *Config) error {
	if c.Dir == "" {
		return errors.New("The 'dir' flag/key is required")
	}

	if c.MaxSize <= 0 {
		return errors.New("The 'max_size' flag/key must be set to a value > 0")
	}

	if c.Port == 0 {
		return errors.New("A valid 'port' flag/key must be specified")
	}

	if c.GRPCPort < 0 {
		return errors.New("The 'grpc_port' flag/key must be 0 (disabled) or a positive integer")
	}

	if c.GRPCPort == 0 && c.ExperimentalRemoteAssetAPI {
		return errors.New("Remote Asset API support depends on gRPC being enabled")
	}

	if (c.TLSCertFile != "" && c.TLSKeyFile == "") || (c.TLSCertFile == "" && c.TLSKeyFile != "") {
		return errors.New("When enabling TLS one must specify both " +
			"'tls_key_file' and 'tls_cert_file'")
	}

	if c.TLSCaFile != "" && (c.TLSCertFile == "" || c.TLSKeyFile == "") {
		return errors.New("When enabling mTLS (authenticating client " +
			"certificates) the server must have it's own 'tls_key_file' " +
			"and 'tls_cert_file' specified.")
	}

	if c.GoogleCloudStorage != nil && c.HTTPBackend != nil && c.S3CloudStorage != nil {
		return errors.New("One can specify at most one proxying backend")
	}

	if c.GoogleCloudStorage != nil {
		if c.GoogleCloudStorage.Bucket == "" {
			return errors.New("The 'bucket' field is required for 'gcs_proxy'")
		}
	}

	if c.HTTPBackend != nil {
		if c.HTTPBackend.BaseURL == "" {
			return errors.New("The 'url' field is required for 'http_proxy'")
		}
	}

	if c.S3CloudStorage != nil {
		if c.S3CloudStorage.AccessKeyID != "" && c.S3CloudStorage.IAMRoleEndpoint != "" {
			return errors.New("Expected either 's3.access_key_id' or 's3.iam_role_endpoint', found both")
		}

		if c.S3CloudStorage.KeyVersion < 1 || c.S3CloudStorage.KeyVersion > 2 {
			return fmt.Errorf("s3.key_version must be either 1 or 2, found %d", c.S3CloudStorage.KeyVersion)
		}
	}

	if c.MetricsDurationBuckets != nil {
		duplicates := make(map[float64]bool)
		for _, bucket := range c.MetricsDurationBuckets {
			_, dupe := duplicates[bucket]
			if dupe {
				return errors.New("'endpoint_metrics_duration_buckets' must not contain duplicate buckets")
			}
			duplicates[bucket] = true
		}
	}

	return nil
}
