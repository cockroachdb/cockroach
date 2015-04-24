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

	"github.com/cockroachdb/cockroach/util"
)

const (
	// EmbeddedCertsDir is the certs directory inside embedded assets.
	EmbeddedCertsDir = "test_certs"
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

// LoadTLSConfigFromDir creates a TLSConfig by loading our keys and certs from the
// specified directory. The directory must contain the following files:
// - ca.crt   -- the certificate of the cluster CA
// - node.crt -- the certificate of this node; should be signed by the CA
// - node.key -- the private key of this node
// If the path is prefixed with "embedded=", load the embedded certs.
func LoadTLSConfigFromDir(certDir string) (*tls.Config, error) {
	certPEM, err := readFileFn(path.Join(certDir, "node.crt"))
	if err != nil {
		return nil, err
	}
	keyPEM, err := readFileFn(path.Join(certDir, "node.key"))
	if err != nil {
		return nil, err
	}
	caPEM, err := readFileFn(path.Join(certDir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	return LoadTLSConfig(certPEM, keyPEM, caPEM)
}

// LoadTLSConfig creates a TLSConfig from the supplied byte strings containing
// - the certificate of the cluster CA,
// - the certificate of this node (should be signed by the CA),
// - the private key of this node.
func LoadTLSConfig(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()

	if ok := certPool.AppendCertsFromPEM(caPEM); !ok {
		err = util.Error("failed to parse PEM data to pool")
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		// TODO(marc): no client certs for now. Even specifying VerifyClientCertIfGiven
		// causes issues with various browsers. We should switch to
		// tls.RequireAndVerifyClientCert once client certs are properly set.
		ClientAuth: tls.NoClientCert,
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

// LoadInsecureTLSConfig creates a TLSConfig that disables TLS.
func LoadInsecureTLSConfig() *tls.Config {
	return nil
}

// LoadClientTLSConfigFromDir creates a client TLSConfig by loading the root CA certs from the
// specified directory. The directory must contain ca.crt.
func LoadClientTLSConfigFromDir(certDir string) (*tls.Config, error) {
	caPEM, err := readFileFn(path.Join(certDir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	return LoadClientTLSConfig(caPEM)
}

// LoadClientTLSConfig creates a client TLSConfig from the supplied byte strings containing
// the certificate of the cluster CA.
func LoadClientTLSConfig(caPEM []byte) (*tls.Config, error) {
	certPool := x509.NewCertPool()

	if ok := certPool.AppendCertsFromPEM(caPEM); !ok {
		err := util.Error("failed to parse PEM data to pool")
		return nil, err
	}

	return &tls.Config{
		RootCAs: certPool,
		// TODO(marc): remove once we have a certificate deployment story in place.
		InsecureSkipVerify: true,

		// Use only TLS v1.2
		MinVersion: tls.VersionTLS12,
	}, nil
}

// LoadInsecureClientTLSConfig creates a TLSConfig that disables TLS.
func LoadInsecureClientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
	}
}
