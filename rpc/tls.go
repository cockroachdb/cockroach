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
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/rpc"
	"path"
	"sync"

	"github.com/cockroachdb/cockroach/rpc/rpctest"
	"github.com/cockroachdb/cockroach/util"
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

// LoadTLSConfigFromDir creates a TLSConfig by loading our keys and certs from the
// specified directory. The directory must contain the following files:
// - ca.crt   -- the certificate of the cluster CA
// - node.crt -- the certificate of this node; should be signed by the CA
// - node.key -- the private key of this node
func LoadTLSConfigFromDir(certDir string) (*TLSConfig, error) {
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
	caPEM, err := ioutil.ReadFile(path.Join(certDir, "ca.crt"))
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

// LoadTestTLSConfig loads the test TLSConfig included with the project. It requires
// a path to the project root, loading the certs from assets bundled with the test.
// TODO Maybe instead of returning err, take a testing.T?  And move to tls_test?
func LoadTestTLSConfig() (*TLSConfig, error) {
	certDir := "./test_certs"
	certPEM, err := rpctest.Asset(path.Join(certDir, "node.crt"))
	if err != nil {
		return nil, err
	}
	keyPEM, err := rpctest.Asset(path.Join(certDir, "node.key"))
	if err != nil {
		return nil, err
	}
	caPEM, err := rpctest.Asset(path.Join(certDir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	return LoadTLSConfig(certPEM, keyPEM, caPEM)
}

// LoadInsecureClientTLSConfig creates a TLSConfig that disables TLS.
func LoadInsecureClientTLSConfig() *TLSConfig {
	return &TLSConfig{
		config: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
}

// tlsListen wraps either net.Listen or crypto/tls.Listen, depending on the contents of
// the passed TLSConfig.
func tlsListen(network, address string, config *TLSConfig) (net.Listener, error) {
	cfg := config.Config()
	if cfg == nil {
		if network != "unix" {
			log.Warningf("listening via %s to %s without TLS", network, address)
		}
		return net.Listen(network, address)
	}
	return tls.Listen(network, address, cfg)
}

// tlsDial wraps either net.Dial or crypto/tls.Dial, depending on the contents of
// the passed TLSConfig.
func tlsDial(network, address string, config *TLSConfig) (net.Conn, error) {
	cfg := config.Config()
	if cfg == nil {
		if network != "unix" {
			log.Warningf("connecting via %s to %s without TLS", network, address)
		}
		return net.Dial(network, address)
	}
	return tls.Dial(network, address, cfg)
}

// tlsDialHTTP connects to an HTTP RPC server at the specified address.
func tlsDialHTTP(network, address string, config *TLSConfig) (net.Conn, error) {
	conn, err := tlsDial(network, address, config)
	if err != nil {
		return conn, err
	}

	// Note: this code was adapted from net/rpc.DialHTTPPath.
	io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")

	// Require successful HTTP response before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return conn, nil
	}
	if err == nil {
		err = util.Errorf("unexpected HTTP response: %s", resp.Status)
	}
	conn.Close()
	return nil, err
}
