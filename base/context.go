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
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/cli/cliflags"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
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

	// NetworkTimeout is the timeout used for network operations.
	NetworkTimeout = 3 * time.Second
)

type lazyTLSConfig struct {
	once      sync.Once
	tlsConfig *tls.Config
	err       error
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

	// clientTLSConfig is the loaded client tlsConfig. It is initialized lazily.
	clientTLSConfig lazyTLSConfig

	// serverTLSConfig is the loaded server tlsConfig. It is initialized lazily.
	serverTLSConfig lazyTLSConfig

	// httpClient is a lazily-initialized http client.
	// It should be accessed through Context.GetHTTPClient() which will
	// initialize if needed.
	httpClient *http.Client
	// Protects httpClient.
	httpClientMu sync.Mutex
}

// InitDefaults sets up the default values for a context.
func (ctx *Context) InitDefaults() {
	ctx.Insecure = defaultInsecure
	ctx.User = defaultUser
}

// HTTPRequestScheme returns "http" or "https" based on the value of Insecure.
func (ctx *Context) HTTPRequestScheme() string {
	if ctx.Insecure {
		return httpScheme
	}
	return httpsScheme
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
				ctx.clientTLSConfig.err = util.Errorf("error setting up client TLS config: %s", ctx.clientTLSConfig.err)
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
				ctx.serverTLSConfig.err = util.Errorf("error setting up client TLS config: %s", ctx.serverTLSConfig.err)
			}
		} else {
			ctx.serverTLSConfig.err = util.Errorf("--%s=false, but --%s is empty. Certificates must be specified.",
				cliflags.InsecureName, cliflags.CertName)
		}
	})

	return ctx.serverTLSConfig.tlsConfig, ctx.serverTLSConfig.err
}

// GetHTTPClient returns the context http client, initializing it
// if needed. It uses the context client TLS config.
func (ctx *Context) GetHTTPClient() (*http.Client, error) {
	ctx.httpClientMu.Lock()
	defer ctx.httpClientMu.Unlock()

	if ctx.httpClient != nil {
		return ctx.httpClient, nil
	}

	tlsConfig, err := ctx.GetClientTLSConfig()
	if err != nil {
		return nil, err
	}
	ctx.httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: NetworkTimeout,
	}

	return ctx.httpClient, nil
}
