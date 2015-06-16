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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package base

import (
	"crypto/tls"
	"net/http"
	"sync"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Base context defaults.
const (
	defaultInsecure = false
	defaultCertsDir = "certs"
	defaultUser     = security.RootUser
	plainScheme     = "http"
	sslScheme       = "https"
)

// Context is embedded by server.Context. A base context is not meant to be
// used directly, but embedding contexts should call ctx.InitDefaults().
type Context struct {
	// Insecure specifies whether to use SSL or not.
	// This is really not recommended.
	Insecure bool

	// Certs specifies a directory containing RSA key and x509 certs.
	// Required unless Insecure is true.
	Certs string

	// User running this process. It could be the user under which
	// the server is running ("node"), or the user passed in client calls.
	User string

	// clientTLSConfig is the loaded client tlsConfig. It is initialized lazily.
	clientTLSConfig *tls.Config
	// serverTLSConfig is the loaded server tlsConfig. It is initialized lazily.
	serverTLSConfig *tls.Config
	// Protects both clientTLSConfig and serverTLSConfig.
	tlsConfigMu sync.Mutex

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
	ctx.Certs = defaultCertsDir
	ctx.User = defaultUser
}

// RequestScheme returns "http" or "https" based on the value of Insecure.
func (ctx *Context) RequestScheme() string {
	if ctx.Insecure {
		return plainScheme
	}
	return sslScheme
}

// GetClientTLSConfig returns the context client TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise load a config based
// on the Certs directory. If Certs is empty, use a very permissive config.
// TODO(marc): empty Certs dir should fail when client certificates are required.
func (ctx *Context) GetClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if ctx.Insecure {
		return nil, nil
	}

	ctx.tlsConfigMu.Lock()
	defer ctx.tlsConfigMu.Unlock()

	if ctx.clientTLSConfig != nil {
		return ctx.clientTLSConfig, nil
	}

	if ctx.Certs != "" {
		if log.V(1) {
			log.Infof("setting up TLS from certificates directory: %s", ctx.Certs)
		}
		cfg, err := security.LoadClientTLSConfig(ctx.Certs, ctx.User)
		if err != nil {
			return nil, util.Errorf("error setting up client TLS config: %s", err)
		}
		ctx.clientTLSConfig = cfg
	} else {
		if log.V(1) {
			log.Infof("no certificates directory specified: using insecure TLS")
		}
		ctx.clientTLSConfig = security.LoadInsecureClientTLSConfig()
	}

	return ctx.clientTLSConfig, nil
}

// GetServerTLSConfig returns the context server TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise load a config based
// on the Certs directory. Fails if Insecure=false and Certs="".
func (ctx *Context) GetServerTLSConfig() (*tls.Config, error) {
	// Early out.
	if ctx.Insecure {
		return nil, nil
	}

	ctx.tlsConfigMu.Lock()
	defer ctx.tlsConfigMu.Unlock()

	if ctx.serverTLSConfig != nil {
		return ctx.serverTLSConfig, nil
	}

	if ctx.Certs == "" {
		return nil, util.Errorf("--insecure=false, but --certs is empty. We need a certs directory")
	}

	if log.V(1) {
		log.Infof("setting up TLS from certificates directory: %s", ctx.Certs)
	}
	cfg, err := security.LoadServerTLSConfig(ctx.Certs)
	if err != nil {
		return nil, util.Errorf("error setting up server TLS config: %s", err)
	}
	ctx.serverTLSConfig = cfg

	return ctx.serverTLSConfig, nil
}

// GetHTTPClient returns the context http client, initializing it
// if needed. It uses the context client TLS config.
func (ctx *Context) GetHTTPClient() (*http.Client, error) {
	ctx.httpClientMu.Lock()
	defer ctx.httpClientMu.Unlock()

	if ctx.httpClient != nil {
		return ctx.httpClient, nil
	}

	if ctx.Insecure {
		log.Warning("running in insecure mode, this is strongly discouraged. See --insecure and --certs.")
	}
	tlsConfig, err := ctx.GetClientTLSConfig()
	if err != nil {
		return nil, err
	}
	ctx.httpClient = &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}

	return ctx.httpClient, nil
}
