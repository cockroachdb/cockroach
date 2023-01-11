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

// EmbeddedTenantIDs lists the tenants we embed certs for.
// See 'securitytest/test_certs/regenerate.sh'.
var EmbeddedTenantIDs = func() []uint64 { return []uint64{10, 11, 20, 2} }

// newServerTLSConfig creates a server TLSConfig from the supplied byte strings containing
// - the certificate of this node (should be signed by the CA),
// - the private key of this node.
// - the certificate of the cluster CA, used to verify other server certificates
// - the certificate of the client CA, used to verify client certificates
//
// caPEM and caClientPEMs can be equal to caPEM (shared CA) or nil (use system CA
// pool).
func newServerTLSConfig(
	settings TLSSettings, certPEM, keyPEM, caPEM []byte, caClientPEMs ...[]byte,
) (*tls.Config, error) {
	cfg, err := newBaseTLSConfigWithCertificate(settings, certPEM, keyPEM, caPEM)
	if err != nil {
		return nil, err
	}
	cfg.ClientAuth = tls.VerifyClientCertIfGiven

	if len(caClientPEMs) != 0 {
		certPool := x509.NewCertPool()
		for _, pem := range caClientPEMs {
			if !certPool.AppendCertsFromPEM(pem) {
				return nil, errors.Errorf("failed to parse client CA PEM data to pool")
			}
		}
		cfg.ClientCAs = certPool
	}

	if settings.oldCipherSuitesEnabled() {
		cfg.CipherSuites = append(
			cfg.CipherSuites,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		)
	}

	// Should we disable session resumption? This may break forward secrecy.
	// cfg.SessionTicketsDisabled = true
	return cfg, nil
}

// newUIServerTLSConfig creates a server TLSConfig for the Admin UI. It does not
// use client authentication or a CA.
// It needs:
// - the server certificate (should be signed by the CA used by HTTP clients to the admin UI)
// - the private key for the certificate
func newUIServerTLSConfig(settings TLSSettings, certPEM, keyPEM []byte) (*tls.Config, error) {
	cfg, err := newBaseTLSConfigWithCertificate(settings, certPEM, keyPEM, nil)
	if err != nil {
		return nil, err
	}

	// Should we disable session resumption? This may break forward secrecy.
	// cfg.SessionTicketsDisabled = true
	return cfg, nil
}

// newClientTLSConfig creates a client TLSConfig from the supplied byte strings containing:
// - the certificate of this client (should be signed by the CA),
// - the private key of this client.
// - the certificate of the cluster CA (use system cert pool if nil)
func newClientTLSConfig(settings TLSSettings, certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	return newBaseTLSConfigWithCertificate(settings, certPEM, keyPEM, caPEM)
}

// newUIClientTLSConfig creates a client TLSConfig to talk to the Admin UI.
// It does not include client certificates and takes an optional CA certificate.
func newUIClientTLSConfig(settings TLSSettings, caPEM []byte) (*tls.Config, error) {
	return newBaseTLSConfig(settings, caPEM)
}

// newBaseTLSConfigWithCertificate returns a tls.Config initialized with the
// passed-in certificate and optional CA certificate.
func newBaseTLSConfigWithCertificate(
	settings TLSSettings, certPEM, keyPEM, caPEM []byte,
) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	cfg, err := newBaseTLSConfig(settings, caPEM)
	if err != nil {
		return nil, err
	}

	cfg.Certificates = []tls.Certificate{cert}
	return cfg, nil
}

// newBaseTLSConfig returns a tls.Config. If caPEM != nil, it is set in RootCAs.
func newBaseTLSConfig(settings TLSSettings, caPEM []byte) (*tls.Config, error) {
	var certPool *x509.CertPool
	if caPEM != nil {
		certPool = x509.NewCertPool()

		if !certPool.AppendCertsFromPEM(caPEM) {
			return nil, errors.Errorf("failed to parse PEM data to pool")
		}
	}

	return &tls.Config{
		RootCAs: certPool,

		VerifyPeerCertificate: makeOCSPVerifier(settings),

		// CipherSuites is a list of enabled TLS 1.2 cipher suites. The
		// order of the list is ignored; prioritization of cipher suites
		// follows hard-coded rules in the Go standard library[1]. Note
		// that TLS 1.3 ciphersuites are not configurable.
		//
		// This is the subset of Go's default cipher suite list which are
		// also marked as "recommended" by IETF[2] (As of June 1, 2022).
		// Mozilla recommends the same list with some comments on
		// rationale and compatibility[3]. These ciphers are recommended
		// because they are the ones that provide forward secrecy and
		// authenticated encryption (AEAD). Mozilla claims they are
		// compatible with "nearly all" clients from the last five years.
		//
		// [1]: https://github.com/golang/go/blob/4aa1efed4853ea067d665a952eee77c52faac774/src/crypto/tls/cipher_suites.go#L215-L270
		// [2]: https://www.iana.org/assignments/tls-parameters/tls-parameters.xhtml#tls-parameters-4
		// [3]: https://wiki.mozilla.org/Security/Server_Side_TLS#Intermediate_compatibility_.28recommended.29
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
			// Note: the codec names
			// TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305
			// and
			// TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305
			// are merely aliases for the two above.
			//
			// NB: no need to add TLS 1.3 ciphers here. As per the
			// documentation of CipherSuites, the TLS 1.3 ciphers are not
			// configurable. Go's predefined list always applies. All TLS
			// 1.3 ciphers meet the forward secrecy and authenticated
			// encryption requirements mentioned above.
		},

		MinVersion: tls.VersionTLS12,
	}, nil
}
