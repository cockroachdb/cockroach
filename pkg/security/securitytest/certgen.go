// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package securitytest

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	keySize      = 2048
	caLifetime   = 10 * 365 * 24 * time.Hour // 10 years
	certLifetime = 5 * 365 * 24 * time.Hour  // 5 years

	// validFrom offsets the NotBefore by one day to handle clock skew.
	validFrom    = -24 * time.Hour
	maxPathLen   = 1
	caCommonName = "Cockroach CA"
)

var (
	certOnce sync.Once
	certMap  map[string][]byte
)

// generatedCerts returns the lazily-initialized map of cert filename to PEM
// bytes. All certs are generated on first call and cached for the process
// lifetime. The returned map must not be modified by callers.
func generatedCerts() map[string][]byte {
	certOnce.Do(func() {
		m, err := generateAllCerts()
		if err != nil {
			panic(fmt.Sprintf(
				"securitytest: failed to generate test certificates: %v", err))
		}
		certMap = m
	})
	return certMap
}

// newSerial generates a random 128-bit serial number for a certificate.
func newSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

// newCertTemplate creates a base certificate template with the given
// CommonName, lifetime, and optional OrganizationalUnit values.
func newCertTemplate(
	cn string, lifetime time.Duration, orgUnits ...string,
) (*x509.Certificate, error) {
	serial, err := newSerial()
	if err != nil {
		return nil, err
	}
	now := timeutil.Now()
	return &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization:       []string{"Cockroach"},
			OrganizationalUnit: orgUnits,
			CommonName:         cn,
		},
		NotBefore: now.Add(validFrom),
		NotAfter:  now.Add(lifetime),
		KeyUsage:  x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
	}, nil
}

// addHosts adds IP and DNS SANs to a certificate template.
func addHosts(template *x509.Certificate, hosts []string) {
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}
}

// rsaKeyToPEM encodes an RSA private key as a PEM block.
func rsaKeyToPEM(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
}

// ed25519KeyToPEM encodes an ed25519 private key as a PKCS#8 PEM block.
func ed25519KeyToPEM(key ed25519.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: der,
	}), nil
}

// certToPEM encodes DER certificate bytes as a PEM block.
func certToPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// generateAllCerts creates all test certificates in memory and returns a map
// from path (e.g. "test_certs/ca.crt") to PEM-encoded bytes.
//
// This replaces the previously-static embedded certificates that required
// periodic manual regeneration. All cert generation uses Go's standard
// crypto libraries directly for simplicity and to avoid coupling to the
// internal security package API.
func generateAllCerts() (map[string][]byte, error) {
	m := make(map[string][]byte)

	// Helper to store an RSA cert and key pair.
	store := func(name string, certDER []byte, key *rsa.PrivateKey) {
		m["test_certs/"+name+".crt"] = certToPEM(certDER)
		m["test_certs/"+name+".key"] = rsaKeyToPEM(key)
	}

	// 1. CA certificate.
	caKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, err
	}
	caDER, err := generateCA(caKey, caLifetime)
	if err != nil {
		return nil, err
	}
	store("ca", caDER, caKey)
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}

	// 2. Tenant CA certificate (separate CA for tenant client connections).
	tenantCAKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, err
	}
	tenantCADER, err := generateCA(tenantCAKey, caLifetime)
	if err != nil {
		return nil, err
	}
	store("ca-client-tenant", tenantCADER, tenantCAKey)
	tenantCACert, err := x509.ParseCertificate(tenantCADER)
	if err != nil {
		return nil, err
	}

	// 3. Node certificate.
	hosts := []string{"127.0.0.1", "::1", "localhost", "*.local"}
	nodeKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, err
	}
	nodeDER, err := generateServerCert(caCert, caKey, nodeKey, hosts)
	if err != nil {
		return nil, err
	}
	store("node", nodeDER, nodeKey)

	// 4. Client certificates: root, testuser, testuser2, testuser3.
	for _, u := range []string{"root", "testuser", "testuser2", "testuser3"} {
		clientKey, err := rsa.GenerateKey(rand.Reader, keySize)
		if err != nil {
			return nil, err
		}
		clientDER, err := generateClientCert(caCert, caKey, clientKey, u)
		if err != nil {
			return nil, err
		}
		store("client."+u, clientDER, clientKey)
	}

	// 5. Tenant client certificates.
	for _, tid := range EmbeddedTenantIDs() {
		tenantKey, err := rsa.GenerateKey(rand.Reader, keySize)
		if err != nil {
			return nil, err
		}
		tenantDER, err := generateTenantCert(
			tenantCACert, tenantCAKey, tenantKey, tid, hosts)
		if err != nil {
			return nil, err
		}
		store(fmt.Sprintf("client-tenant.%d", tid), tenantDER, tenantKey)
	}

	// 6. Tenant signing certificates (self-signed, ed25519 keys).
	for _, tid := range EmbeddedTenantIDs() {
		pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		signingDER, err := generateTenantSigningCert(pubKey, privKey, tid)
		if err != nil {
			return nil, err
		}
		keyPEM, err := ed25519KeyToPEM(privKey)
		if err != nil {
			return nil, err
		}
		name := fmt.Sprintf("tenant-signing.%d", tid)
		m["test_certs/"+name+".crt"] = certToPEM(signingDER)
		m["test_certs/"+name+".key"] = keyPEM
	}

	// 7. CN/SAN variant client certificates for auth tests.
	if err := generateCNSANCerts(m, caCert, caKey); err != nil {
		return nil, err
	}

	return m, nil
}

// generateCA creates a self-signed CA certificate.
func generateCA(signer crypto.Signer, lifetime time.Duration) ([]byte, error) {
	template, err := newCertTemplate(caCommonName, lifetime)
	if err != nil {
		return nil, err
	}
	template.BasicConstraintsValid = true
	template.IsCA = true
	template.MaxPathLen = maxPathLen
	template.KeyUsage |= x509.KeyUsageCertSign | x509.KeyUsageContentCommitment
	return x509.CreateCertificate(
		rand.Reader, template, template, signer.Public(), signer)
}

// generateServerCert creates a node server/client certificate signed by the CA.
func generateServerCert(
	caCert *x509.Certificate, caKey *rsa.PrivateKey, nodeKey *rsa.PrivateKey, hosts []string,
) ([]byte, error) {
	template, err := newCertTemplate("node", certLifetime)
	if err != nil {
		return nil, err
	}
	template.ExtKeyUsage = []x509.ExtKeyUsage{
		x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth,
	}
	addHosts(template, hosts)
	return x509.CreateCertificate(
		rand.Reader, template, caCert, nodeKey.Public(), caKey)
}

// generateClientCert creates a client certificate for the given user, signed
// by the CA.
func generateClientCert(
	caCert *x509.Certificate, caKey *rsa.PrivateKey, clientKey *rsa.PrivateKey, user string,
) ([]byte, error) {
	template, err := newCertTemplate(user, certLifetime)
	if err != nil {
		return nil, err
	}
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	return x509.CreateCertificate(
		rand.Reader, template, caCert, clientKey.Public(), caKey)
}

// generateTenantCert creates a tenant client certificate with the "Tenants"
// OrganizationalUnit, signed by the tenant CA.
func generateTenantCert(
	caCert *x509.Certificate,
	caKey *rsa.PrivateKey,
	tenantKey *rsa.PrivateKey,
	tenantID uint64,
	hosts []string,
) ([]byte, error) {
	template, err := newCertTemplate(
		fmt.Sprintf("%d", tenantID), certLifetime, "Tenants")
	if err != nil {
		return nil, err
	}
	template.ExtKeyUsage = []x509.ExtKeyUsage{
		x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth,
	}
	addHosts(template, hosts)
	return x509.CreateCertificate(
		rand.Reader, template, caCert, tenantKey.Public(), caKey)
}

// generateTenantSigningCert creates a self-signed tenant signing certificate
// using ed25519 keys.
func generateTenantSigningCert(
	pubKey ed25519.PublicKey, privKey ed25519.PrivateKey, tenantID uint64,
) ([]byte, error) {
	template, err := newCertTemplate(
		fmt.Sprintf("Tenant %d Token Signing Certificate", tenantID),
		certLifetime,
	)
	if err != nil {
		return nil, err
	}
	template.BasicConstraintsValid = true
	template.IsCA = false
	template.KeyUsage = x509.KeyUsageDigitalSignature
	return x509.CreateCertificate(
		rand.Reader, template, template, pubKey, privKey)
}

// generateCNSANCerts creates the three special client certificates that test
// different CN/SAN configurations:
//   - testuser_cn_only: CommonName="testuser", no DNS SAN
//   - testuser_san_only: no CommonName, DNS SAN="testuser"
//   - testuser_cn_and_san: CommonName="testuser" and DNS SAN="testuser"
func generateCNSANCerts(
	m map[string][]byte, caCert *x509.Certificate, caKey *rsa.PrivateKey,
) error {
	type cnSANSpec struct {
		name     string
		cn       string
		dnsNames []string
	}

	specs := []cnSANSpec{
		{name: "client.testuser_cn_only", cn: "testuser", dnsNames: nil},
		{name: "client.testuser_san_only", cn: "", dnsNames: []string{"testuser"}},
		{name: "client.testuser_cn_and_san", cn: "testuser", dnsNames: []string{"testuser"}},
	}

	for _, spec := range specs {
		key, err := rsa.GenerateKey(rand.Reader, keySize)
		if err != nil {
			return err
		}

		template, err := newCertTemplate(spec.cn, certLifetime)
		if err != nil {
			return err
		}
		template.DNSNames = spec.dnsNames
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}

		certDER, err := x509.CreateCertificate(
			rand.Reader, template, caCert, key.Public(), caKey)
		if err != nil {
			return err
		}

		m["test_certs/"+spec.name+".crt"] = certToPEM(certDER)
		m["test_certs/"+spec.name+".key"] = rsaKeyToPEM(key)
	}

	return nil
}
