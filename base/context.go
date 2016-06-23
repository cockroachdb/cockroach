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
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/cli/cliflags"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util/retry"
)

// Base context defaults.
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
	DefaultRaftTickInterval = 100 * time.Millisecond
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

// Context is embedded by server.Context. A base context is not meant to be
// used directly, but embedding contexts should call ctx.InitDefaults().
type Context struct {
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

	// Addr is the server's public address.
	Addr string

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

// InitDefaults sets up the default values for a context.
func (ctx *Context) InitDefaults() {
	ctx.Insecure = defaultInsecure
	ctx.User = defaultUser
	ctx.Addr = defaultAddr
	ctx.HTTPAddr = defaultHTTPAddr
}

// HTTPRequestScheme returns "http" or "https" based on the value of Insecure.
func (ctx *Context) HTTPRequestScheme() string {
	if ctx.Insecure {
		return httpScheme
	}
	return httpsScheme
}

// AdminURL returns the URL for the admin UI.
func (ctx *Context) AdminURL() string {
	return fmt.Sprintf("%s://%s", ctx.HTTPRequestScheme(), ctx.HTTPAddr)
}

// PGURL returns the URL for the postgres endpoint.
func (ctx *Context) PGURL(user string) (*url.URL, error) {
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
	if ctx.Insecure {
		options.Add("sslmode", "disable")
	} else {
		options.Add("sslmode", "verify-full")
		requiredFlags := []struct {
			name     string
			value    string
			flagName string
		}{
			{"sslcert", ctx.SSLCert, cliflags.CertName},
			{"sslkey", ctx.SSLCertKey, cliflags.KeyName},
			{"sslrootcert", ctx.SSLCA, cliflags.CACertName},
		}
		for _, c := range requiredFlags {
			if c.value == "" {
				return nil, fmt.Errorf("missing --%s flag", c.flagName)
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
		User:     url.User(user),
		Host:     ctx.Addr,
		RawQuery: options.Encode(),
	}, nil
}

// GetClientTLSConfig returns the context client TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise load a config based
// on the SSLCert file. If SSLCert is empty, use a very permissive config.
// TODO(marc): empty SSLCert should fail when client certificates are required.
func (ctx *Context) GetClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if ctx.Insecure {
		return nil, nil
	}

	ctx.clientTLSConfig.once.Do(func() {
		if ctx.SSLCert != "" {
			ctx.clientTLSConfig.tlsConfig, ctx.clientTLSConfig.err = security.LoadClientTLSConfig(
				ctx.SSLCA, ctx.SSLCert, ctx.SSLCertKey)
			if ctx.clientTLSConfig.err != nil {
				ctx.clientTLSConfig.err = errors.Errorf("error setting up client TLS config: %s", ctx.clientTLSConfig.err)
			}
		} else {
			log.Println("no certificates specified: using insecure TLS")
			ctx.clientTLSConfig.tlsConfig = security.LoadInsecureClientTLSConfig()
		}
	})

	return ctx.clientTLSConfig.tlsConfig, ctx.clientTLSConfig.err
}

// GetServerTLSConfig returns the context server TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise load a config based
// on the SSLCert file. Fails if Insecure=false and SSLCert="".
func (ctx *Context) GetServerTLSConfig() (*tls.Config, error) {
	// Early out.
	if ctx.Insecure {
		return nil, nil
	}

	ctx.serverTLSConfig.once.Do(func() {
		if ctx.SSLCert != "" {
			ctx.serverTLSConfig.tlsConfig, ctx.serverTLSConfig.err = security.LoadServerTLSConfig(
				ctx.SSLCA, ctx.SSLCert, ctx.SSLCertKey)
			if ctx.serverTLSConfig.err != nil {
				ctx.serverTLSConfig.err = errors.Errorf("error setting up server TLS config: %s", ctx.serverTLSConfig.err)
			}
		} else {
			ctx.serverTLSConfig.err = errors.Errorf("--%s=false, but --%s is empty. Certificates must be specified.",
				cliflags.InsecureName, cliflags.CertName)
		}
	})

	return ctx.serverTLSConfig.tlsConfig, ctx.serverTLSConfig.err
}

// GetHTTPClient returns the context http client, initializing it
// if needed. It uses the context client TLS config.
func (ctx *Context) GetHTTPClient() (http.Client, error) {
	ctx.httpClient.once.Do(func() {
		ctx.httpClient.httpClient.Timeout = NetworkTimeout
		var transport http.Transport
		ctx.httpClient.httpClient.Transport = &transport
		transport.TLSClientConfig, ctx.httpClient.err = ctx.GetClientTLSConfig()
	})

	return ctx.httpClient.httpClient, ctx.httpClient.err
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
