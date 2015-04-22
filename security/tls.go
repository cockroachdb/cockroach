// Copyright 2014 The Cockroach Authors.
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
// Author: jqmp (jaqueramaphan@gmail.com)

package security

// TODO(jqmp): The use of TLS here is just a proof of concept; its security
// properties haven't been analyzed or audited.

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"path"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/util"
)

const (
	EmbeddedPrefix = "embedded:"
)

// TLSConfig contains the TLS settings for a Cockroach node. Currently it's
// just a wrapper for tls.Config. If config is nil, we don't use TLS.
type TLSConfig struct {
	sync.Mutex
	config *tls.Config
}

// Config returns a copy of the TLS configuration.
func (c *TLSConfig) Config() *tls.Config {
	c.Lock()
	defer c.Unlock()
	if c.config == nil {
		return nil
	}
	cc := *c.config
	return &cc
}

// LoadTLSConfigFromDir creates a TLSConfig by loading our keys and certs from the
// specified directory. The directory must contain the following files:
// - ca.crt   -- the certificate of the cluster CA
// - node.crt -- the certificate of this node; should be signed by the CA
// - node.key -- the private key of this node
// If the path is prefixed with "embedded:", load the embedded certs.
func LoadTLSConfigFromDir(certDir string) (*TLSConfig, error) {
	if strings.HasPrefix(certDir, EmbeddedPrefix) {
		return LoadTestTLSConfig(certDir[len(EmbeddedPrefix):])
	}
	certPEM, err := ioutil.ReadFile(path.Join(certDir, "node.crt"))
	if err != nil {
		return nil, err
	}
	keyPEM, err := ioutil.ReadFile(path.Join(certDir, "node.key"))
	if err != nil {
		return nil, err
	}
	caPEM, err := ioutil.ReadFile(path.Join(certDir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	return LoadTLSConfig(certPEM, keyPEM, caPEM)
}

// LoadTestTLSConfig loads the embedded certs. This is only called from
// LoadTLSConfigFromDir when the certdir path starts with "embedded:".
func LoadTestTLSConfig(certDir string) (*TLSConfig, error) {
	certPEM, err := securitytest.Asset(path.Join(certDir, "node.crt"))
	if err != nil {
		return nil, err
	}
	keyPEM, err := securitytest.Asset(path.Join(certDir, "node.key"))
	if err != nil {
		return nil, err
	}
	caPEM, err := securitytest.Asset(path.Join(certDir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	return LoadTLSConfig(certPEM, keyPEM, caPEM)
}

// LoadTLSConfig creates a TLSConfig from the supplied byte strings containing
// - the certificate of the cluster CA,
// - the certificate of this node (should be signed by the CA),
// - the private key of this node.
func LoadTLSConfig(certPEM, keyPEM, caPEM []byte) (*TLSConfig, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()

	if ok := certPool.AppendCertsFromPEM(caPEM); !ok {
		err = util.Error("failed to parse PEM data to pool")
		return nil, err
	}

	return &TLSConfig{
		config: &tls.Config{
			Certificates: []tls.Certificate{cert},
			// TODO(marc): clients are bad about this. We should switch to
			// tls.RequireAndVerifyClientCert once client certs are properly set.
			ClientAuth: tls.VerifyClientCertIfGiven,
			RootCAs:    certPool,
			ClientCAs:  certPool,

			// Use the default cipher suite from golang (RC4 is going away in 1.5).
			// Prefer the server-specified suite.
			PreferServerCipherSuites: true,

			// Lots of things don't support 1.2. Let's try 1.1.
			MinVersion: tls.VersionTLS11,

			// Should we disable session resumption? This may break forward secrecy.
			// SessionTicketsDisabled: true,
		},
	}, nil
}

// LoadInsecureTLSConfig creates a TLSConfig that disables TLS.
func LoadInsecureTLSConfig() *TLSConfig {
	return &TLSConfig{
		config: nil,
	}
}

// LoadClientTLSConfigFromDir creates a client TLSConfig by loading the root CA certs from the
// specified directory. The directory must contain ca.crt.
func LoadClientTLSConfigFromDir(certDir string) (*TLSConfig, error) {
	if strings.HasPrefix(certDir, EmbeddedPrefix) {
		return LoadTestClientTLSConfig(certDir[len(EmbeddedPrefix):])
	}
	caPEM, err := ioutil.ReadFile(path.Join(certDir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	return LoadClientTLSConfig(caPEM)
}

// LoadTestClientTLSConfig loads the embedded certs. This is only called from
// LoadClientTLSConfigFromDir when the certdir path starts with "embedded:".
func LoadTestClientTLSConfig(certDir string) (*TLSConfig, error) {
	caPEM, err := securitytest.Asset(path.Join(certDir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	return LoadClientTLSConfig(caPEM)
}

// LoadClientTLSConfig creates a client TLSConfig from the supplied byte strings containing
// the certificate of the cluster CA.
func LoadClientTLSConfig(caPEM []byte) (*TLSConfig, error) {
	certPool := x509.NewCertPool()

	if ok := certPool.AppendCertsFromPEM(caPEM); !ok {
		err := util.Error("failed to parse PEM data to pool")
		return nil, err
	}

	return &TLSConfig{
		config: &tls.Config{
			RootCAs: certPool,
			// TODO(marc): remove once we have a certificate deployment story in place.
			InsecureSkipVerify: true,

			// Use only TLS v1.2
			MinVersion: tls.VersionTLS12,
		},
	}, nil
}

// LoadInsecureClientTLSConfig creates a TLSConfig that disables TLS.
func LoadInsecureClientTLSConfig() *TLSConfig {
	return &TLSConfig{
		config: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
}
