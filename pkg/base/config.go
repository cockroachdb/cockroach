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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
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
)

type lazyTLSConfig struct {
	once      sync.Once
	tlsConfig *tls.Config
	err       error
}

type lazyHTTPClient struct {
	once       sync.Once
	httpClient http.Client
	err        error
}

// Config is embedded by server.Config. A base config is not meant to be used
// directly, but embedding configs should call cfg.InitDefaults().
type Config struct {
	// Insecure specifies whether to use SSL or not.
	// This is really not recommended.
	Insecure bool

	// SSLCA and others contain the paths to the ssl certificates and keys.
	SSLCA      string // CA certificate
	SSLCAKey   string // CA key (to sign only)
	SSLCert    string // Client/server certificate
	SSLCertKey string // Client/server key

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

	// clientTLSConfig is the loaded client TLS config. It is initialized lazily.
	clientTLSConfig lazyTLSConfig

	// serverTLSConfig is the loaded server TLS config. It is initialized lazily.
	serverTLSConfig lazyTLSConfig

	// httpClient uses the client TLS config. It is initialized lazily.
	httpClient lazyHTTPClient
}

// InitDefaults sets up the default values for a config.
func (cfg *Config) InitDefaults() {
	cfg.Insecure = defaultInsecure
	cfg.User = defaultUser
	cfg.Addr = defaultAddr
	cfg.AdvertiseAddr = cfg.Addr
	cfg.HTTPAddr = defaultHTTPAddr
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
		if cfg.SSLCA == "" {
			return nil, fmt.Errorf("missing --%s flag", cliflags.CACert.Name)
		}

		// Check that cfg.SSLCert and cfg.SSLCertKey are either both empty or
		// both non-empty.
		// If both are provided, the server will authenticate the client using
		// certificate authentication. If not, password authentication will be
		// used.
		if cfg.SSLCert == "" && cfg.SSLCertKey != "" {
			return nil, fmt.Errorf("missing --%s flag", cliflags.Cert.Name)
		}
		if cfg.SSLCertKey == "" && cfg.SSLCert != "" {
			return nil, fmt.Errorf("missing --%s flag", cliflags.Key.Name)
		}

		options.Add("sslmode", "verify-full")
		sslFlags := []struct {
			name     string
			value    string
			flagName string
		}{
			{"sslcert", cfg.SSLCert, cliflags.Cert.Name},
			{"sslkey", cfg.SSLCertKey, cliflags.Key.Name},
			{"sslrootcert", cfg.SSLCA, cliflags.CACert.Name},
		}

		for _, c := range sslFlags {
			if c.value == "" {
				continue
			}
			path := absPath(c.value)
			if _, err := os.Stat(path); err != nil {
				return nil, fmt.Errorf("file for --%s flag gave error: %v", c.flagName, err)
			}
			options.Add(c.name, path)
		}
	}
	return &url.URL{
		Scheme:   "postgresql",
		User:     user,
		Host:     cfg.AdvertiseAddr,
		RawQuery: options.Encode(),
	}, nil
}

// GetClientTLSConfig returns the client TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise load a config based
// on the SSLCert file. If SSLCert is empty, use a very permissive config.
// TODO(marc): empty SSLCert should fail when client certificates are required.
func (cfg *Config) GetClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if cfg.Insecure {
		return nil, nil
	}

	cfg.clientTLSConfig.once.Do(func() {
		cfg.clientTLSConfig.tlsConfig, cfg.clientTLSConfig.err = security.LoadClientTLSConfig(
			cfg.SSLCA, cfg.SSLCert, cfg.SSLCertKey)
		if cfg.clientTLSConfig.err != nil {
			cfg.clientTLSConfig.err = errors.Errorf("error setting up client TLS config: %s", cfg.clientTLSConfig.err)
		}
	})

	return cfg.clientTLSConfig.tlsConfig, cfg.clientTLSConfig.err
}

// GetServerTLSConfig returns the server TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise load a config based
// on the SSLCert file. Fails if Insecure=false and SSLCert="".
func (cfg *Config) GetServerTLSConfig() (*tls.Config, error) {
	// Early out.
	if cfg.Insecure {
		return nil, nil
	}

	cfg.serverTLSConfig.once.Do(func() {
		if cfg.SSLCert != "" {
			cfg.serverTLSConfig.tlsConfig, cfg.serverTLSConfig.err = security.LoadServerTLSConfig(
				cfg.SSLCA, cfg.SSLCert, cfg.SSLCertKey)
			if cfg.serverTLSConfig.err != nil {
				cfg.serverTLSConfig.err = errors.Errorf("error setting up server TLS config: %s", cfg.serverTLSConfig.err)
			}
		} else {
			cfg.serverTLSConfig.err = errors.Errorf("--%s=false, but --%s is empty. Certificates must be specified.",
				cliflags.Insecure.Name, cliflags.Cert.Name)
		}
	})

	return cfg.serverTLSConfig.tlsConfig, cfg.serverTLSConfig.err
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
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2,
	}
}
