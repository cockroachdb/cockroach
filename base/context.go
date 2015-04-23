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

// GetHTTPClient returns the context http client, initializing it
// if needed. It uses the context Certs.
func (ctx *Context) GetHTTPClient() (*http.Client, error) {
	ctx.httpClientMu.Lock()
	defer ctx.httpClientMu.Unlock()

	if ctx.httpClient != nil {
		return ctx.httpClient, nil
	}

	var tlsConfig *tls.Config
	if ctx.Certs == "" {
		log.V(1).Infof("no certificates directory specified: using insecure TLS")
		tlsConfig = security.LoadInsecureClientTLSConfig().Config()
	} else {
		log.V(1).Infof("setting up TLS from certificates directory: %s", ctx.Certs)
		cfg, err := security.LoadClientTLSConfigFromDir(ctx.Certs)
		if err != nil {
			return nil, util.Errorf("error setting up client TLS config: %s", err)
		}
		tlsConfig = cfg.Config()
	}
	ctx.httpClient = &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}

	return ctx.httpClient, nil
}
