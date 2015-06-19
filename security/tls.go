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
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/gogo/protobuf/proto"
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

// LoadServerTLSConfig creates a server TLSConfig by loading our keys and certs from the
// specified directory. The directory must contain the following files:
// - ca.crt   -- the certificate of the cluster CA
// - node.server.crt -- the server certificate of this node; should be signed by the CA
// - node.server.key -- the certificate key
// If the path is prefixed with "embedded=", load the embedded certs.
// We should never have username != "node", but this is a good way to
// catch tests that use the wrong users.
func LoadServerTLSConfig(certDir, username string) (*tls.Config, error) {
	certPEM, err := readFileFn(path.Join(certDir, username+".server.crt"))
	if err != nil {
		return nil, err
	}
	keyPEM, err := readFileFn(path.Join(certDir, username+".server.key"))
	if err != nil {
		return nil, err
	}
	caPEM, err := readFileFn(path.Join(certDir, "ca.crt"))
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
		err = util.Error("failed to parse PEM data to pool")
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

// LoadInsecureTLSConfig creates a TLSConfig that disables TLS.
func LoadInsecureTLSConfig() *tls.Config {
	return nil
}

// LoadClientTLSConfig creates a client TLSConfig by loading the CA and client certs
// from the specified directory. The directory must contain the following files:
// - ca.crt   -- the certificate of the cluster CA
// - <username>.client.crt -- the client certificate of this client; should be signed by the CA
// - <username>.client.key -- the certificate key
// If the path is prefixed with "embedded=", load the embedded certs.
func LoadClientTLSConfig(certDir, username string) (*tls.Config, error) {
	certPEM, err := readFileFn(path.Join(certDir, clientCertFile(username)))
	if err != nil {
		return nil, err
	}
	keyPEM, err := readFileFn(path.Join(certDir, clientKeyFile(username)))
	if err != nil {
		return nil, err
	}
	caPEM, err := readFileFn(path.Join(certDir, "ca.crt"))
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
		err := util.Error("failed to parse PEM data to pool")
		return nil, err
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

// LogRequestCertificates examines a http request and logs a summary of the TLS config.
func LogRequestCertificates(r *http.Request) {
	LogTLSState(fmt.Sprintf("%s %s", r.Method, r.URL), r.TLS)
}

func LogTLSState(method string, tlsState *tls.ConnectionState) {
	if tlsState == nil {
		if log.V(3) {
			log.Infof("%s: no TLS", method)
		}
		return
	}

	peerCerts := []string{}
	verifiedChain := []string{}
	for _, cert := range tlsState.PeerCertificates {
		peerCerts = append(peerCerts, cert.Subject.CommonName)
	}
	for _, chain := range tlsState.VerifiedChains {
		subjects := []string{}
		for _, cert := range chain {
			subjects = append(subjects, cert.Subject.CommonName)
		}
		verifiedChain = append(verifiedChain, strings.Join(subjects, ","))
	}
	if log.V(3) {
		log.Infof("%s: peer certs: %v, chain: %v", method, peerCerts, verifiedChain)
	}
}

// GetCertificateUser extract the username from a client certificate.
func GetCertificateUser(tlsState *tls.ConnectionState) (string, error) {
	if tlsState == nil {
		return "", util.Errorf("request is not using TLS")
	}
	if len(tlsState.PeerCertificates) == 0 {
		return "", util.Errorf("no client certificates in request")
	}
	if len(tlsState.VerifiedChains) != len(tlsState.PeerCertificates) {
		// TODO(marc): can this happen?
		return "", util.Errorf("client cerficates not verified")
	}
	return tlsState.PeerCertificates[0].Subject.CommonName, nil
}

// AuthenticationHook builds an authentication hook based on the
// security mode and client certificate.
// Must be called at connection time and passed the TLS state.
// Returns a func(proto.Message) error. The passed-in proto must implement
// the GetUser interface.
func AuthenticationHook(insecureMode bool, tlsState *tls.ConnectionState) (
	func(request proto.Message) error, error) {
	var certUser string
	var err error

	if !insecureMode {
		certUser, err = GetCertificateUser(tlsState)
		if err != nil {
			return nil, err
		}
	}

	return func(request proto.Message) error {
		// userRequest is an interface for RPC requests that have a "requested user".
		type userRequest interface {
			// GetUser returns the user from the request.
			GetUser() string
		}

		// UserRequest must be implemented.
		requestWithUser, ok := request.(userRequest)
		if !ok {
			return util.Errorf("unknown request type: %T", request)
		}

		// Extract user and verify.
		// TODO(marc): we may eventually need stricter user syntax rules.
		requestedUser := requestWithUser.GetUser()
		if len(requestedUser) == 0 {
			return util.Errorf("missing User in request: %+v", request)
		}

		// If running in insecure mode, we have nothing to verify it against.
		if insecureMode {
			return nil
		}

		// The client certificate user must either be "node", or match the requested used.
		if certUser == NodeUser || certUser == requestedUser {
			return nil
		}
		return util.Errorf("requested user is %s, but certificate is for %s", requestedUser, certUser)
	}, nil
}
