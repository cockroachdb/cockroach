// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dyncert

import (
	"crypto/tls"
	"crypto/x509"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// dynamicTLS provides TLS configuration that supports certificate rotation.
//
// The underlying Cert objects can be updated via Set(), and dynamicTLS will
// automatically detect changes and rebuild TLS objects on the next handshake.
// This allows certificate rotation without restarting services or reconnecting.
//
// Thread-safe for concurrent use.
type dynamicTLS struct {
	localCert          *Cert  // This endpoint's certificate and key (may be nil)
	peerCA             *Cert  // CA certificate for verifying peer (may be nil)
	peerCN             string // Required peer CN (empty = no check)
	peerOU             string // Required peer OU (empty = no check)
	insecureSkipVerify bool   // Skip all verification including CN/OU (testing only)

	mu struct {
		syncutil.Mutex
		// Derived TLS objects, rebuilt when certs change.
		tlsCert      *tls.Certificate // Also used for change detection via pointer comparison
		rootCAs      *x509.CertPool   // Also used for change detection via pointer comparison
		serverConfig *tls.Config      // For GetConfigForClient
	}
}

// TLSOption configures a dynamic TLS config.
type TLSOption func(*dynamicTLS)

// connectionVerifier is a function that verifies a TLS connection.
type connectionVerifier func(tls.ConnectionState) error

// WithPeerCN requires the peer certificate to have the specified Common Name.
func WithPeerCN(cn string) TLSOption {
	return func(d *dynamicTLS) {
		d.peerCN = cn
	}
}

// WithPeerOU requires the peer certificate to have the specified Organizational Unit.
func WithPeerOU(ou string) TLSOption {
	return func(d *dynamicTLS) {
		d.peerOU = ou
	}
}

// WithInsecureSkipVerify skips all server certificate verification, including
// chain, hostname, CN, and OU checks. This should only be used for testing.
func WithInsecureSkipVerify() TLSOption {
	return func(d *dynamicTLS) {
		d.insecureSkipVerify = true
	}
}

// NewTLSConfig creates a tls.Config with dynamic certificate support.
// The returned config can be used for client, server, or peer-to-peer connections.
//
// Parameters:
//   - localCert: This endpoint's certificate and key (may be nil)
//   - peerCA: CA for verifying the peer's certificate (may be nil)
//
// Client behavior:
//   - If localCert is provided, presents it to the server when requested
//   - If peerCA is provided, verifies the server's certificate against it
//     (supports dynamic CA rotation)
//   - If peerCA is nil, uses Go's default verification against system CAs
//
// Server behavior:
//   - If localCert is provided, presents it to connecting clients
//   - If peerCA is provided, requires and verifies client certificates (mTLS)
//   - If peerCA is nil, does not request client certificates
//
// Options:
//   - WithPeerCN(cn): Requires peer certificate to have the specified CN
//   - WithPeerOU(ou): Requires peer certificate to have the specified OU
//   - WithInsecureSkipVerify(): Skips all server certificate verification (testing only)
func NewTLSConfig(localCert, peerCA *Cert, opts ...TLSOption) *tls.Config {
	d := &dynamicTLS{
		localCert: localCert,
		peerCA:    peerCA,
	}
	for _, opt := range opts {
		opt(d)
	}

	config := &tls.Config{}

	// Client-side: present our certificate when requested.
	if localCert != nil {
		config.GetClientCertificate = d.getClientCertificate
	}

	// Client-side: build chain of connection verifiers. This is never used in
	// the server-side case because GetConfigForClient returns a different config.
	// Each verifier is independent and can be composed.
	var verifiers []connectionVerifier
	if d.insecureSkipVerify {
		// Skip verification of the server certificate (testing only).
		config.InsecureSkipVerify = true
	} else {
		if peerCA != nil {
			// Verify peer certificate chain against peerCA.
			// Replace Go's built-in verification with our own to support dynamic CA
			// rotation.
			config.InsecureSkipVerify = true
			verifiers = append(verifiers, d.verifyChain)
		}

		if d.peerCN != "" || d.peerOU != "" {
			verifiers = append(verifiers, d.verifyNames)
		}
	}

	// Set up VerifyConnection if we have any custom verifiers.
	if len(verifiers) > 0 {
		config.VerifyConnection = func(cs tls.ConnectionState) error {
			for _, v := range verifiers {
				if err := v(cs); err != nil {
					return err
				}
			}
			return nil
		}
	}

	// Server-side: return dynamic config for each connection.
	// Only set if we have a local cert to present.
	if localCert != nil {
		config.GetConfigForClient = d.getConfigForClient
	}

	return config
}

// getClientCertificate returns the current local certificate for TLS handshakes.
// Used by clients to present their certificate to servers.
func (d *dynamicTLS) getClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.refreshLocalCertLocked(); err != nil {
		return nil, err
	}
	return d.mu.tlsCert, nil
}

// getConfigForClient returns a server TLS config for the incoming connection.
// The returned config presents localCert and verifies clients against peerCA.
func (d *dynamicTLS) getConfigForClient(*tls.ClientHelloInfo) (*tls.Config, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.refreshServerConfigLocked(); err != nil {
		return nil, err
	}
	return d.mu.serverConfig, nil
}

// verifyChain performs certificate chain and hostname verification.
// This replicates Go's default verification logic for verifying the server
// certificate, including:
//   - Certificate chain validation against the root CA pool
//   - Hostname verification
func (d *dynamicTLS) verifyChain(cs tls.ConnectionState) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("no peer certificate provided")
	}

	// Get current CA pool (refreshes if cert changed).
	rootCAs, err := d.rootCAs()
	if err != nil {
		return err
	}

	// Build intermediate pool from remaining certificates in chain.
	var intermediates *x509.CertPool
	if len(cs.PeerCertificates) > 1 {
		intermediates = x509.NewCertPool()
		for _, cert := range cs.PeerCertificates[1:] {
			intermediates.AddCert(cert)
		}
	}

	// Verify the certificate chain with full options matching Go's default.
	opts := x509.VerifyOptions{
		Roots:         rootCAs,
		Intermediates: intermediates,
		DNSName:       cs.ServerName,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if _, err := cs.PeerCertificates[0].Verify(opts); err != nil {
		return errors.Wrap(err, "verifying peer certificate")
	}

	return nil
}

// verifyNames verifies that the peer certificate has the expected CN and OU.
func (d *dynamicTLS) verifyNames(cs tls.ConnectionState) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("no peer certificate provided")
	}
	return d.checkNames(cs.PeerCertificates[0])
}

// checkNames checks that the certificate has the expected CN and OU.
// This helper is shared between client-side and server-side verification.
func (d *dynamicTLS) checkNames(cert *x509.Certificate) error {
	if d.peerCN != "" && cert.Subject.CommonName != d.peerCN {
		return errors.Errorf("peer certificate CN=%q, expected %q",
			cert.Subject.CommonName, d.peerCN)
	}
	if d.peerOU != "" && !slices.Contains(cert.Subject.OrganizationalUnit, d.peerOU) {
		return errors.Errorf("peer certificate OU=%v, expected %q",
			cert.Subject.OrganizationalUnit, d.peerOU)
	}
	return nil
}

// rootCAs returns the current CA certificate pool, refreshing if needed.
func (d *dynamicTLS) rootCAs() (*x509.CertPool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.refreshPeerCALocked(); err != nil {
		return nil, err
	}
	return d.mu.rootCAs, nil
}

// refreshLocalCertLocked checks if the local cert has changed and rebuilds
// the TLS certificate if needed. Caller must hold mu.
func (d *dynamicTLS) refreshLocalCertLocked() error {
	tlsCert, err := d.localCert.AsTLSCertificate()
	if err != nil {
		return errors.Wrapf(err, "parsing local certificate")
	}

	// Check if cert changed (pointer comparison works because AsTLSCertificate caches).
	if d.mu.tlsCert == tlsCert {
		return nil
	}

	d.mu.tlsCert = tlsCert
	d.mu.serverConfig = nil // Invalidate server config
	return nil
}

// refreshPeerCALocked checks if the peer CA has changed and rebuilds
// the root CA pool if needed. Caller must hold mu.
func (d *dynamicTLS) refreshPeerCALocked() error {
	rootCAs, err := d.peerCA.AsCAPool()
	if err != nil {
		return errors.Wrapf(err, "parsing CA certificate")
	}

	// Check if pool changed (pointer comparison works because AsCAPool caches).
	if d.mu.rootCAs == rootCAs {
		return nil
	}

	d.mu.rootCAs = rootCAs
	d.mu.serverConfig = nil // Invalidate server config
	return nil
}

// refreshServerConfigLocked rebuilds the server config if needed.
// Caller must hold mu.
func (d *dynamicTLS) refreshServerConfigLocked() error {
	// Refresh dependencies first.
	if err := d.refreshLocalCertLocked(); err != nil {
		return err
	}
	if d.peerCA != nil {
		if err := d.refreshPeerCALocked(); err != nil {
			return err
		}
	}

	// Check if we need to rebuild.
	if d.mu.serverConfig != nil {
		return nil
	}

	// Build server config with local certificate.
	cfg := &tls.Config{
		Certificates: []tls.Certificate{*d.mu.tlsCert},
	}

	// Configure client certificate verification if peerCA is provided (mTLS).
	if d.peerCA != nil {
		cfg.ClientCAs = d.mu.rootCAs
		cfg.ClientAuth = tls.RequireAndVerifyClientCert

		if d.peerCN != "" || d.peerOU != "" {
			cfg.VerifyPeerCertificate = func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
				// Standard verification already passed; just check CN/OU.
				if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
					return errors.New("no verified certificate chain")
				}
				return d.checkNames(verifiedChains[0][0])
			}
		}
	}

	d.mu.serverConfig = cfg
	return nil
}
