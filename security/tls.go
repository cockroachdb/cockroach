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
// permissions and limitations under the License.
//
// Author: jqmp (jaqueramaphan@gmail.com)

package security

// TODO(jqmp): The use of TLS here is just a proof of concept; its security
// properties haven't been analyzed or audited.

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/cockroachdb/cockroach/util"
)

// EmbeddedCertsDir is the certs directory inside embedded assets.
// Embedded*{Cert,Key} are the filenames for embedded certs.
const (
	EmbeddedCertsDir     = "test_certs"
	EmbeddedCACert       = "ca.crt"
	EmbeddedCAKey        = "ca.key"
	EmbeddedNodeCert     = "node.crt"
	EmbeddedNodeKey      = "node.key"
	EmbeddedRootCert     = "root.crt"
	EmbeddedRootKey      = "root.key"
	EmbeddedTestUserCert = "testuser.crt"
	EmbeddedTestUserKey  = "testuser.key"
)

// readFileFn is used to mock out file system access during tests.
var readFileFn = ioutil.ReadFile

// SetReadFileFn allows to switch out ioutil.ReadFile by a mock
// for testing purposes.
func SetReadFileFn(f func(string) ([]byte, error)) {
	readFileFn = f
}

// ResetReadFileFn is the counterpart to SetReadFileFn, restoring the
// original behaviour for loading certificate related data from disk.
func ResetReadFileFn() {
	readFileFn = ioutil.ReadFile
}

// LoadServerTLSConfig creates a server TLSConfig by loading the CA and server certs.
// The following paths must be passed:
// - sslCA: path to the CA certificate
// - sslCert: path to the server certificate
// - sslCertKey: path to the server key
// If the path is prefixed with "embedded=", load the embedded certs.
func LoadServerTLSConfig(sslCA, sslCert, sslCertKey string) (*tls.Config, error) {
	certPEM, err := readFileFn(sslCert)
	if err != nil {
		return nil, err
	}
	keyPEM, err := readFileFn(sslCertKey)
	if err != nil {
		return nil, err
	}
	caPEM, err := readFileFn(sslCA)
	if err != nil {
		return nil, err
	}
	return newServerTLSConfig(certPEM, keyPEM, caPEM)
}

// newServerTLSConfig creates a server TLSConfig from the supplied byte strings containing
// - the certificate of this node (should be signed by the CA),
// - the private key of this node.
// - the certificate of the cluster CA,
func newServerTLSConfig(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()

	if ok := certPool.AppendCertsFromPEM(caPEM); !ok {
		err = util.Errorf("failed to parse PEM data to pool")
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		// Verify client certs if passed.
		ClientAuth: tls.VerifyClientCertIfGiven,
		RootCAs:    certPool,
		ClientCAs:  certPool,

		// Use the default cipher suite from golang (RC4 is going away in 1.5).
		// Prefer the server-specified suite.
		PreferServerCipherSuites: true,

		// TLS 1.1 and 1.2 support is crappy out there. Let's use 1.0.
		MinVersion: tls.VersionTLS10,

		// Should we disable session resumption? This may break forward secrecy.
		// SessionTicketsDisabled: true,
	}, nil
}

// LoadClientTLSConfig creates a client TLSConfig by loading the CA and client certs.
// The following paths must be passed:
// - sslCA: path to the CA certificate
// - sslCert: path to the client certificate
// - sslCertKey: path to the client key
// If the path is prefixed with "embedded=", load the embedded certs.
func LoadClientTLSConfig(sslCA, sslCert, sslCertKey string) (*tls.Config, error) {
	certPEM, err := readFileFn(sslCert)
	if err != nil {
		return nil, err
	}
	keyPEM, err := readFileFn(sslCertKey)
	if err != nil {
		return nil, err
	}
	caPEM, err := readFileFn(sslCA)
	if err != nil {
		return nil, err
	}

	return newClientTLSConfig(certPEM, keyPEM, caPEM)
}

// newClientTLSConfig creates a client TLSConfig from the supplied byte strings containing:
// - the certificate of this client (should be signed by the CA),
// - the private key of this client.
// - the certificate of the cluster CA,
func newClientTLSConfig(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()

	if ok := certPool.AppendCertsFromPEM(caPEM); !ok {
		return nil, util.Errorf("failed to parse PEM data to pool")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      certPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// LoadInsecureClientTLSConfig creates a TLSConfig that disables TLS.
func LoadInsecureClientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
	}
}
