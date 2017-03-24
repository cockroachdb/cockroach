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
	"net/http"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/pkg/errors"
)

// Base config defaults.
const (
	defaultInsecure = false
	defaultUser     = security.RootUser
	httpScheme      = "http"
	httpsScheme     = "https"

	// NetworkTimeout is the timeout used for network operations.
	NetworkTimeout = 3 * time.Second

	// DefaultRaftTickInterval is the default resolution of the Raft timer.
	DefaultRaftTickInterval = 200 * time.Millisecond
)

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

	// HistogramWindowInterval is used to determine the approximate length of time
	// that individual samples are retained in in-memory histograms. Currently,
	// it is set to the arbitrary length of six times the Metrics sample interval.
	// See the comment in server.Config for more details.
	HistogramWindowInterval time.Duration
}

// InitDefaults sets up the default values for a config.
func (cfg *Config) InitDefaults() {
	cfg.Insecure = defaultInsecure
	cfg.User = defaultUser
}

// HTTPRequestScheme returns "http" or "https" based on the value of Insecure.
func (cfg *Config) HTTPRequestScheme() string {
	if cfg.Insecure {
		return httpScheme
	}
	return httpsScheme
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

// ConnectingHelper helps with making network requests using the authentication
// information from a Config.
type ConnectingHelper struct {
	cfg Config

	// clientTLSConfig is the loaded client TLS config. It is initialized lazily.
	clientTLSConfig lazyTLSConfig

	// serverTLSConfig is the loaded server TLS config. It is initialized lazily.
	serverTLSConfig lazyTLSConfig

	// httpClient uses the client TLS config. It is initialized lazily.
	httpClient lazyHTTPClient
}

// NewConnectingHelper creates a ConnectingHelper.
func NewConnectingHelper(cfg Config) *ConnectingHelper {
	return &ConnectingHelper{cfg: cfg}
}

// GetClientTLSConfig returns the client TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise load a config based
// on the SSLCert file. If SSLCert is empty, use a very permissive config.
// TODO(marc): empty SSLCert should fail when client certificates are required.
func (c *ConnectingHelper) GetClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if c.cfg.Insecure {
		return nil, nil
	}

	c.clientTLSConfig.once.Do(func() {
		c.clientTLSConfig.tlsConfig, c.clientTLSConfig.err = security.LoadClientTLSConfig(
			c.cfg.SSLCA, c.cfg.SSLCert, c.cfg.SSLCertKey)
		if c.clientTLSConfig.err != nil {
			c.clientTLSConfig.err = errors.Errorf(
				"error setting up client TLS config: %s", c.clientTLSConfig.err)
		}
	})

	return c.clientTLSConfig.tlsConfig, c.clientTLSConfig.err
}

// GetServerTLSConfig returns the server TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise load a config based
// on the SSLCert file. Fails if Insecure=false and SSLCert="".
func (c *ConnectingHelper) GetServerTLSConfig() (*tls.Config, error) {
	// Early out.
	if c.cfg.Insecure {
		return nil, nil
	}

	c.serverTLSConfig.once.Do(func() {
		if c.cfg.SSLCert != "" {
			c.serverTLSConfig.tlsConfig, c.serverTLSConfig.err = security.LoadServerTLSConfig(
				c.cfg.SSLCA, c.cfg.SSLCert, c.cfg.SSLCertKey)
			if c.serverTLSConfig.err != nil {
				c.serverTLSConfig.err = errors.Errorf("error setting up server TLS config: %s", c.serverTLSConfig.err)
			}
		} else {
			c.serverTLSConfig.err = errors.Errorf("--%s=false, but --%s is empty. Certificates must be specified.",
				cliflags.Insecure.Name, cliflags.Cert.Name)
		}
	})

	return c.serverTLSConfig.tlsConfig, c.serverTLSConfig.err
}

// GetHTTPClient returns the http client, initializing it
// if needed. It uses the client TLS config.
func (c *ConnectingHelper) GetHTTPClient() (http.Client, error) {
	c.httpClient.once.Do(func() {
		c.httpClient.httpClient.Timeout = NetworkTimeout
		var transport http.Transport
		c.httpClient.httpClient.Transport = &transport
		transport.TLSClientConfig, c.httpClient.err = c.GetClientTLSConfig()
	})

	return c.httpClient.httpClient, c.httpClient.err
}

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
