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

package rpc

// TODO(jqmp): The use of TLS here is just a proof of concept; its security
// properties haven't been analyzed or audited.

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"net"
	"path"
	"sync"

	"github.com/cockroachdb/cockroach/util/log"
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

// LoadTLSConfig creates a TLSConfig by loading our keys and certs from the
// specified directory. The directory must contain the following files:
// - ca.crt   -- the certificate of the cluster CA
// - node.crt -- the certificate of this node; should be signed by the CA
// - node.key -- the private key of this node
func LoadTLSConfig(certDir string) (*TLSConfig, error) {
	cert, err := tls.LoadX509KeyPair(
		path.Join(certDir, "node.crt"),
		path.Join(certDir, "node.key"),
	)
	if err != nil {
		log.Info(err)
		return nil, err
	}

	certPool := x509.NewCertPool()
	pemData, err := ioutil.ReadFile(path.Join(certDir, "ca.crt"))
	if err != nil {
		log.Info(err)
		return nil, err
	}

	if ok := certPool.AppendCertsFromPEM(pemData); !ok {
		err = errors.New("failed to parse PEM data to pool")
		log.Info(err)
		return nil, err
	}

	return &TLSConfig{
		config: &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			RootCAs:      certPool,
			ClientCAs:    certPool,

			// TODO(jqmp): Set CipherSuites?
			// TODO(jqmp): Set MinVersion?
		},
	}, nil
}

// LoadInsecureTLSConfig creates a TLSConfig that disables TLS.
func LoadInsecureTLSConfig() *TLSConfig {
	return &TLSConfig{
		config: nil,
	}
}

// LoadTestTLSConfig loads the test TLSConfig included with the project. It requires
// a path to the project root.
// TODO Maybe instead of returning err, take a testing.T?  And move to tls_test?
func LoadTestTLSConfig(projectRoot string) (*TLSConfig, error) {
	return LoadTLSConfig(path.Join(projectRoot, "resources", "test_certs"))
}

// tlsListen wraps either net.Listen or crypto/tls.Listen, depending on the contents of
// the passed TLSConfig.
func tlsListen(network string, address string, config *TLSConfig) (net.Listener, error) {
	cfg := config.Config()
	if cfg == nil {
		if network != "unix" {
			log.Warningf("Listening via %s to %s without TLS", network, address)
		}
		return net.Listen(network, address)
	}
	return tls.Listen(network, address, cfg)
}

// tlsDial wraps either net.Dial or crypto/tls.Dial, depending on the contents of
// the passed TLSConfig.
func tlsDial(network string, address string, config *TLSConfig) (net.Conn, error) {
	cfg := config.Config()
	if cfg == nil {
		if network != "unix" {
			log.Warningf("Connecting via %s to %s without TLS", network, address)
		}
		return net.Dial(network, address)
	}
	return tls.Dial(network, address, cfg)
}
