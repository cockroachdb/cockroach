// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package base

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// Base config defaults.
const (
	defaultInsecure = false
	defaultUser     = security.RootUser
	httpScheme      = "http"
	httpsScheme     = "https"

	// From IANA Service Name and Transport Protocol Port Number Registry. See
	// https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml?search=cockroachdb
	DefaultPort = "26257"

	// The default port for HTTP-for-humans.
	DefaultHTTPPort = "8080"

	// NB: net.JoinHostPort is not a constant.
	defaultAddr     = ":" + DefaultPort
	defaultHTTPAddr = ":" + DefaultHTTPPort

	// NetworkTimeout is the timeout used for network operations.
	NetworkTimeout = 3 * time.Second

	// DefaultRaftTickInterval is the default resolution of the Raft timer.
	DefaultRaftTickInterval = 200 * time.Millisecond

	// DefaultCertsDirectory is the default value for the cert directory flag.
	DefaultCertsDirectory = "${HOME}/.cockroach-certs"
)

type lazyHTTPClient struct {
	once       sync.Once
	httpClient http.Client
	err        error
}

type lazyCertificateManager struct {
	once sync.Once
	cm   *security.CertificateManager
	err  error
}

// Config is embedded by server.Config. A base config is not meant to be used
// directly, but embedding configs should call cfg.InitDefaults().
type Config struct {
	// Insecure specifies whether to use SSL or not.
	// This is really not recommended.
	Insecure bool

	// SSLCAKey is used to sign new certs.
	SSLCAKey string
	// SSLCertsDir is the path to the certificate/key directory.
	SSLCertsDir string

	// User running this process. It could be the user under which
	// the server is running or the user passed in client calls.
	User string

	// Addr is the address the server is listening on.
	Addr string

	// AdvertiseAddr is the address advertised by the server to other nodes
	// in the cluster. It should be reachable by all other nodes and should
	// route to an interface that Addr is listening on.
	AdvertiseAddr string

	// HTTPAddr is server's public HTTP address.
	//
	// This is temporary, and will be removed when grpc.(*Server).ServeHTTP
	// performance problems are addressed upstream.
	//
	// See https://github.com/grpc/grpc-go/issues/586.
	HTTPAddr string

	// The certificate manager. Must be accessed through GetCertificateManager.
	certificateManager lazyCertificateManager

	// httpClient uses the client TLS config. It is initialized lazily.
	httpClient lazyHTTPClient

	// HistogramWindowInterval is used to determine the approximate length of time
	// that individual samples are retained in in-memory histograms. Currently,
	// it is set to the arbitrary length of six times the Metrics sample interval.
	// See the comment in server.Config for more details.
	HistogramWindowInterval time.Duration
}

// InitDefaults sets up the default values for a config.
// This is also used in tests to reset global objects.
func (cfg *Config) InitDefaults() {
	cfg.Insecure = defaultInsecure
	cfg.User = defaultUser
	cfg.Addr = defaultAddr
	cfg.AdvertiseAddr = cfg.Addr
	cfg.HTTPAddr = defaultHTTPAddr
	cfg.SSLCertsDir = DefaultCertsDirectory
	cfg.certificateManager = lazyCertificateManager{}
}

// HTTPRequestScheme returns "http" or "https" based on the value of Insecure.
func (cfg *Config) HTTPRequestScheme() string {
	if cfg.Insecure {
		return httpScheme
	}
	return httpsScheme
}

// AdminURL returns the URL for the admin UI.
func (cfg *Config) AdminURL() string {
	return fmt.Sprintf("%s://%s", cfg.HTTPRequestScheme(), cfg.HTTPAddr)
}

// GetClientCertPaths returns the paths to the client certs, specifically:
// CA cert, Client certs, Client key.
func (cfg *Config) GetClientCertPaths(user string) (string, string, string, error) {
	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return "", "", "", err
	}
	return cm.GetClientCertPaths(user)
}

// ClientHasValidCerts returns true if the specified client has valid certs,
// meaning we have a CA cert and a client cert/key pair.
func (cfg *Config) ClientHasValidCerts(user string) bool {
	_, _, _, err := cfg.GetClientCertPaths(user)
	return err == nil
}

// PGURL returns the URL for the postgres endpoint.
func (cfg *Config) PGURL(user *url.Userinfo) (*url.URL, error) {
	// Try to convert path to an absolute path. Failing to do so return path
	// unchanged.
	absPath := func(path string) string {
		r, err := filepath.Abs(path)
		if err != nil {
			return path
		}
		return r
	}

	options := url.Values{}
	if cfg.Insecure {
		options.Add("sslmode", "disable")
	} else {
		caCertPath, certPath, keyPath, err := cfg.GetClientCertPaths(user.Username())
		if err != nil {
			return nil, err
		}
		options.Add("sslmode", "verify-full")
		options.Add("sslrootcert", absPath(caCertPath))
		options.Add("sslcert", absPath(certPath))
		options.Add("sslkey", absPath(keyPath))
	}

	return &url.URL{
		Scheme:   "postgresql",
		User:     user,
		Host:     cfg.AdvertiseAddr,
		RawQuery: options.Encode(),
	}, nil
}

// GetCertificateManager returns the certificate manager, initializing it
// on the first call.
func (cfg *Config) GetCertificateManager() (*security.CertificateManager, error) {
	cfg.certificateManager.once.Do(func() {
		cfg.certificateManager.cm, cfg.certificateManager.err =
			security.NewCertificateManager(cfg.SSLCertsDir)
	})
	return cfg.certificateManager.cm, cfg.certificateManager.err
}

// GetClientTLSConfig returns the client TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise ask the certificate
// manager for a TLS config using certs for the config.User.
func (cfg *Config) GetClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if cfg.Insecure {
		return nil, nil
	}

	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return nil, err
	}
	return cm.GetClientTLSConfig(cfg.User)
}

// GetServerTLSConfig returns the server TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise ask the certificate
// manager for a server TLS config.
func (cfg *Config) GetServerTLSConfig() (*tls.Config, error) {
	// Early out.
	if cfg.Insecure {
		return nil, nil
	}

	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return nil, err
	}

	return cm.GetServerTLSConfig()
}

// GetHTTPClient returns the http client, initializing it
// if needed. It uses the client TLS config.
func (cfg *Config) GetHTTPClient() (http.Client, error) {
	cfg.httpClient.once.Do(func() {
		cfg.httpClient.httpClient.Timeout = NetworkTimeout
		var transport http.Transport
		cfg.httpClient.httpClient.Transport = &transport
		transport.TLSClientConfig, cfg.httpClient.err = cfg.GetClientTLSConfig()
	})

	return cfg.httpClient.httpClient, cfg.httpClient.err
}

// DefaultRetryOptions should be used for retrying most
// network-dependent operations.
func DefaultRetryOptions() retry.Options {
	// TODO(bdarnell): This should vary with network latency.
	// Derive the retry options from a configured or measured
	// estimate of latency.
	return retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     1.5,
	}
}
