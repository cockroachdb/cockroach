// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"crypto/tls"
	"crypto/x509"
	"net"

	"github.com/cockroachdb/errors"
)

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
			OldCipherSuites()...,
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

	if settings.oldCipherSuitesEnabled() {
		cfg.CipherSuites = append(
			cfg.CipherSuites,
			OldCipherSuites()...,
		)
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

		CipherSuites: RecommendedCipherSuites(),

		MinVersion: tls.VersionTLS12,
	}, nil
}

// TLSCipherRestrictLn provides a TLS listener used for restricting tls
// connections. It accepts to a custom function which can be used to intercept
// the connection and validate requirements for TLS like ciphers used.
type TLSCipherRestrictLn struct {
	net.Listener
	fn func(conn net.Conn) error
}

// Accept accepts the connection
func (cl *TLSCipherRestrictLn) Accept() (conn net.Conn, err error) {
	if conn, err = cl.Accept(); err == nil {
		if cl.fn != nil {
			if err := cl.fn(conn); err != nil {
				conn.Close()
				return nil, err
			}
		}
	}
	return
}

// NewTLSCipherRestrictListener initializes a new TLSCipherRestrictLn listener
func NewTLSCipherRestrictListener(ln net.Listener, config *tls.Config) *TLSCipherRestrictLn {
	return &TLSCipherRestrictLn{
		Listener: tls.NewListener(ln, config),
		fn:       TLSCipherRestrict,
	}
}

// Server returns a new TLS connection after validate requirements for TLS like
// ciphers used.
func Server(conn net.Conn, config *tls.Config) (c *tls.Conn, err error) {
	c = tls.Server(conn, config)
	if err := TLSCipherRestrict(c); err != nil {
		c.Close()
		return nil, err
	}
	return
}
