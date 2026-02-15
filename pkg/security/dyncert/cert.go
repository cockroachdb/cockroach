// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dyncert

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Cert holds certificate(s) and an optional private key in PEM format.
//
// This type supports certificate rotation without restarting services.
// By storing certs in a Cert and calling Set() with new data, callers can
// atomically update certificates and have subsequent operations use the new data.
//
// The PEM data may contain:
//   - A single certificate (leaf or CA)
//   - A certificate chain (leaf + intermediates)
//   - Multiple CA certificates
//
// Usage:
//   - AsTLSCertificate() - for leaf certs with private keys (requires keyPEM)
//   - AsCAPool() - for CA certs used in verification (keyPEM optional/ignored)
//
// Parsed forms are cached and invalidated on Set() calls. Pointer stability
// of cached forms enables change detection via pointer comparison.
type Cert struct {
	mu struct {
		syncutil.Mutex
		certPEM []byte
		keyPEM  []byte

		// Cached parsed forms.
		tlsCert *tls.Certificate
		pool    *x509.CertPool
	}
}

// NewCert creates a new Cert with the given PEM-encoded certificate(s) and key.
// Pass nil for keyPEM when the key is not needed (e.g., CA certificates used
// only for verification).
func NewCert(certPEM, keyPEM []byte) *Cert {
	c := &Cert{}
	c.Set(certPEM, keyPEM)
	return c
}

// LoadCert reads certificate and key from disk and returns a new Cert.
// If keyPath is empty, only the certificate is loaded (for CA certs).
func LoadCert(certPath, keyPath string) (*Cert, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, errors.Wrapf(err, "read cert %s", certPath)
	}
	var keyPEM []byte
	if keyPath != "" {
		keyPEM, err = os.ReadFile(keyPath)
		if err != nil {
			return nil, errors.Wrapf(err, "read key %s", keyPath)
		}
	}
	return NewCert(certPEM, keyPEM), nil
}

// Get returns the certificate and key PEM data atomically.
func (c *Cert) Get() (certPEM, keyPEM []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.certPEM, c.mu.keyPEM
}

// Set atomically updates the certificate and key PEM data.
// This clears cached parsed forms, so subsequent accessor calls will
// return new pointers.
func (c *Cert) Set(certPEM, keyPEM []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.certPEM = certPEM
	c.mu.keyPEM = keyPEM
	c.mu.tlsCert = nil
	c.mu.pool = nil
}

// AsTLSCertificate returns the certificate as a tls.Certificate suitable for
// use in TLS handshakes (e.g., GetCertificate, GetClientCertificate callbacks).
//
// Requires both certPEM and keyPEM to be set. Returns an error if the key is missing.
// The Leaf field is populated for efficient access to the parsed x509.Certificate.
//
// The result is cached - repeated calls return the same pointer until Set() is called.
// This pointer stability enables change detection via pointer comparison.
func (c *Cert) AsTLSCertificate() (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.tlsCert != nil {
		return c.mu.tlsCert, nil
	}

	// Parse certificate and key.
	cert, err := tls.X509KeyPair(c.mu.certPEM, c.mu.keyPEM)
	if err != nil {
		return nil, err
	}

	c.mu.tlsCert = &cert
	return c.mu.tlsCert, nil
}

// AsLeafCertificate returns the parsed leaf certificate and private key.
//
// This is a convenience wrapper around AsTLSCertificate that extracts the
// Leaf certificate and PrivateKey. Useful for certificate generation and
// inspection where you need the parsed x509.Certificate and crypto.Signer.
func (c *Cert) AsLeafCertificate() (*x509.Certificate, crypto.Signer, error) {
	tlsCert, err := c.AsTLSCertificate()
	if err != nil {
		return nil, nil, err
	}

	key, ok := tlsCert.PrivateKey.(crypto.Signer)
	if !ok {
		return nil, nil, errors.New("private key is not a crypto.Signer")
	}

	return tlsCert.Leaf, key, nil
}

// AsCAPool returns all certificates as an x509.CertPool suitable for use
// in certificate verification (e.g., RootCAs, ClientCAs in tls.Config).
//
// All PEM blocks are parsed and added to the pool. This is useful for CA
// certificates where multiple trust roots may be concatenated.
//
// The result is cached - repeated calls return the same pointer until Set() is called.
// This pointer stability enables change detection via pointer comparison.
//
// Returns error if any PEM block fails to parse as a certificate.
func (c *Cert) AsCAPool() (*x509.CertPool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.pool != nil {
		return c.mu.pool, nil
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(c.mu.certPEM) {
		return nil, errors.New("failed to parse certificates from PEM")
	}

	c.mu.pool = pool
	return pool, nil
}
