// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/cockroachdb/errors"
)

// EmbeddedCertsDir is the certs directory inside embedded assets.
// Embedded*{Cert,Key} are the filenames for embedded certs.
const (
	EmbeddedCertsDir     = "test_certs"
	EmbeddedCACert       = "ca.crt"
	EmbeddedCAKey        = "ca.key"
	EmbeddedClientCACert = "ca-client.crt"
	EmbeddedClientCAKey  = "ca-client.key"
	EmbeddedUICACert     = "ca-ui.crt"
	EmbeddedUICAKey      = "ca-ui.key"
	EmbeddedNodeCert     = "node.crt"
	EmbeddedNodeKey      = "node.key"
	EmbeddedRootCert     = "client.root.crt"
	EmbeddedRootKey      = "client.root.key"
	EmbeddedTestUserCert = "client.testuser.crt"
	EmbeddedTestUserKey  = "client.testuser.key"
)

// LoadServerTLSConfig creates a server TLSConfig by loading the CA and server certs.
// The following paths must be passed:
// - sslCA: path to the CA certificate
// - sslClientCA: path to the CA certificate to verify client certificates,
//                can be the same as sslCA
// - sslCert: path to the server certificate
// - sslCertKey: path to the server key
// If the path is prefixed with "embedded=", load the embedded certs.
func LoadServerTLSConfig(sslCA, sslClientCA, sslCert, sslCertKey string) (*tls.Config, error) {
	certPEM, err := assetLoaderImpl.ReadFile(sslCert)
	if err != nil {
		return nil, err
	}
	keyPEM, err := assetLoaderImpl.ReadFile(sslCertKey)
	if err != nil {
		return nil, err
	}
	caPEM, err := assetLoaderImpl.ReadFile(sslCA)
	if err != nil {
		return nil, err
	}
	clientCAPEM, err := assetLoaderImpl.ReadFile(sslClientCA)
	if err != nil {
		return nil, err
	}
	return newServerTLSConfig(certPEM, keyPEM, caPEM, clientCAPEM)
}

// newServerTLSConfig creates a server TLSConfig from the supplied byte strings containing
// - the certificate of this node (should be signed by the CA),
// - the private key of this node.
// - the certificate of the cluster CA, used to verify other server certificates
// - the certificate of the client CA, used to verify client certificates
//
// caClientPEM can be equal to caPEM (shared CA) or nil (use system CA pool).
func newServerTLSConfig(certPEM, keyPEM, caPEM, caClientPEM []byte) (*tls.Config, error) {
	cfg, err := newBaseTLSConfigWithCertificate(certPEM, keyPEM, caPEM)
	if err != nil {
		return nil, err
	}
	cfg.ClientAuth = tls.VerifyClientCertIfGiven

	if caClientPEM != nil {
		certPool := x509.NewCertPool()

		if !certPool.AppendCertsFromPEM(caClientPEM) {
			return nil, errors.Errorf("failed to parse client CA PEM data to pool")
		}
		cfg.ClientCAs = certPool
	}

	// Use the default cipher suite from golang (RC4 is going away in 1.5).
	// Prefer the server-specified suite.
	cfg.PreferServerCipherSuites = true
	// Should we disable session resumption? This may break forward secrecy.
	// cfg.SessionTicketsDisabled = true
	return cfg, nil
}

// newUIServerTLSConfig creates a server TLSConfig for the Admin UI. It does not
// use client authentication or a CA.
// It needs:
// - the server certificate (should be signed by the CA used by HTTP clients to the admin UI)
// - the private key for the certificate
func newUIServerTLSConfig(certPEM, keyPEM []byte) (*tls.Config, error) {
	cfg, err := newBaseTLSConfigWithCertificate(certPEM, keyPEM, nil)
	if err != nil {
		return nil, err
	}

	// Use the default cipher suite from golang (RC4 is going away in 1.5).
	// Prefer the server-specified suite.
	cfg.PreferServerCipherSuites = true
	// Should we disable session resumption? This may break forward secrecy.
	// cfg.SessionTicketsDisabled = true
	return cfg, nil
}

// LoadClientTLSConfig creates a client TLSConfig by loading the CA and client certs.
// The following paths must be passed:
// - sslCA: path to the CA certificate
// - sslCert: path to the client certificate
// - sslCertKey: path to the client key
// If the path is prefixed with "embedded=", load the embedded certs.
func LoadClientTLSConfig(sslCA, sslCert, sslCertKey string) (*tls.Config, error) {
	certPEM, err := assetLoaderImpl.ReadFile(sslCert)
	if err != nil {
		return nil, err
	}
	keyPEM, err := assetLoaderImpl.ReadFile(sslCertKey)
	if err != nil {
		return nil, err
	}
	caPEM, err := assetLoaderImpl.ReadFile(sslCA)
	if err != nil {
		return nil, err
	}

	return newClientTLSConfig(certPEM, keyPEM, caPEM)
}

// newClientTLSConfig creates a client TLSConfig from the supplied byte strings containing:
// - the certificate of this client (should be signed by the CA),
// - the private key of this client.
// - the certificate of the cluster CA (use system cert pool if nil)
func newClientTLSConfig(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	return newBaseTLSConfigWithCertificate(certPEM, keyPEM, caPEM)
}

// newUIClientTLSConfig creates a client TLSConfig to talk to the Admin UI.
// It does not include client certificates and takes an optional CA certificate.
func newUIClientTLSConfig(caPEM []byte) (*tls.Config, error) {
	return newBaseTLSConfig(caPEM)
}

// newBaseTLSConfigWithCertificate returns a tls.Config initialized with the
// passed-in certificate and optional CA certificate.
func newBaseTLSConfigWithCertificate(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	cfg, err := newBaseTLSConfig(caPEM)
	if err != nil {
		return nil, err
	}

	cfg.Certificates = []tls.Certificate{cert}
	return cfg, nil
}

// newBaseTLSConfig returns a tls.Config. If caPEM != nil, it is set in RootCAs.
func newBaseTLSConfig(caPEM []byte) (*tls.Config, error) {
	var certPool *x509.CertPool
	if caPEM != nil {
		certPool = x509.NewCertPool()

		if !certPool.AppendCertsFromPEM(caPEM) {
			return nil, errors.Errorf("failed to parse PEM data to pool")
		}
	}

	return &tls.Config{
		RootCAs: certPool,

		// This is Go's default list of cipher suites (as of go 1.8.3),
		// with the following differences:
		// - 3DES-based cipher suites have been removed. This cipher is
		//   vulnerable to the Sweet32 attack and is sometimes reported by
		//   security scanners. (This is arguably a false positive since
		//   it will never be selected: Any TLS1.2 implementation MUST
		//   include at least one cipher higher in the priority list, but
		//   there's also no reason to keep it around)
		// - AES is always prioritized over ChaCha20. Go makes this decision
		//   by default based on the presence or absence of hardware AES
		//   acceleration.
		//   TODO(bdarnell): do the same detection here. See
		//   https://github.com/golang/go/issues/21167
		//
		// Note that some TLS cipher suite guidance (such as Mozilla's[1])
		// recommend replacing the CBC_SHA suites below with CBC_SHA384 or
		// CBC_SHA256 variants. We do not do this because Go does not
		// currerntly implement the CBC_SHA384 suites, and its CBC_SHA256
		// implementation is vulnerable to the Lucky13 attack and is disabled
		// by default.[2]
		//
		// [1]: https://wiki.mozilla.org/Security/Server_Side_TLS#Modern_compatibility
		// [2]: https://github.com/golang/go/commit/48d8edb5b21db190f717e035b4d9ab61a077f9d7
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},

		MinVersion: tls.VersionTLS12,
	}, nil
}
