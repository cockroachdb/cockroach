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
	defaultCertsDir = "certs"
)

// Context is embedded by client.Context and server.Context.
// A base context is not meant to be used directly, but embedding
// contexts should call ctx.InitDefaults().
type Context struct {
	// Certs specifies a directory containing RSA key and x509 certs.
	Certs string

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
	ctx.Certs = defaultCertsDir
}

// GetClientTLSConfig returns the context client TLS config, initializing it
// if needed. It uses the context Certs field.
// If Certs is empty, load insecure configs.
func (ctx *Context) GetClientTLSConfig() (*tls.Config, error) {
	ctx.tlsConfigMu.Lock()
	defer ctx.tlsConfigMu.Unlock()

	if ctx.clientTLSConfig != nil {
		return ctx.clientTLSConfig, nil
	}

	if ctx.Certs == "" {
		log.V(1).Infof("no certificates directory specified: using insecure TLS")
		ctx.clientTLSConfig = security.LoadInsecureClientTLSConfig()
	} else {
		log.V(1).Infof("setting up TLS from certificates directory: %s", ctx.Certs)
		cfg, err := security.LoadClientTLSConfigFromDir(ctx.Certs)
		if err != nil {
			return nil, util.Errorf("error setting up client TLS config: %s", err)
		}
		ctx.clientTLSConfig = cfg
	}

	return ctx.clientTLSConfig, nil
}

// GetServerTLSConfig returns the context server TLS config, initializing it
// if needed. It uses the context Certs field.
// If Certs is empty, load insecure configs.
func (ctx *Context) GetServerTLSConfig() (*tls.Config, error) {
	ctx.tlsConfigMu.Lock()
	defer ctx.tlsConfigMu.Unlock()

	if ctx.serverTLSConfig != nil {
		return ctx.serverTLSConfig, nil
	}

	if ctx.Certs == "" {
		log.V(1).Infof("no certificates directory specified: using insecure TLS")
		ctx.serverTLSConfig = security.LoadInsecureTLSConfig()
	} else {
		log.V(1).Infof("setting up TLS from certificates directory: %s", ctx.Certs)
		cfg, err := security.LoadTLSConfigFromDir(ctx.Certs)
		if err != nil {
			return nil, util.Errorf("error setting up server TLS config: %s", err)
		}
		ctx.serverTLSConfig = cfg
	}

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

	tlsConfig, err := ctx.GetClientTLSConfig()
	if err != nil {
		return nil, err
	}
	ctx.httpClient = &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}

	return ctx.httpClient, nil
}
