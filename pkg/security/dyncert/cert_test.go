// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dyncert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// generateTestCA generates a self-signed CA certificate for testing.
func generateTestCA(t *testing.T, cn string) *Cert {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return NewCert(certPEM, keyPEM)
}

// generateTestLeaf generates a leaf certificate signed by the given CA.
// The CN is set as provided, and "localhost" is always added as a DNS SAN
// to allow hostname verification in tests.
func generateTestLeaf(t *testing.T, ca *Cert, cn string) *Cert {
	t.Helper()
	return generateTestLeafWith(t, ca, cn, nil)
}

// generateTestLeafWith generates a leaf certificate with a callback to modify
// the template before signing.
func generateTestLeafWith(t *testing.T, ca *Cert, cn string, modify func(*x509.Certificate)) *Cert {
	t.Helper()

	caCert, caKey, err := ca.AsLeafCertificate()
	require.NoError(t, err)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	if modify != nil {
		modify(template)
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return NewCert(certPEM, keyPEM)
}

func TestCert(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("NewCert", func(t *testing.T) {
		cert := NewCert([]byte("cert-pem"), []byte("key-pem"))
		certPEM, keyPEM := cert.Get()
		require.Equal(t, []byte("cert-pem"), certPEM)
		require.Equal(t, []byte("key-pem"), keyPEM)
	})

	t.Run("NewCert with nil key", func(t *testing.T) {
		cert := NewCert([]byte("cert-pem"), nil)
		certPEM, keyPEM := cert.Get()
		require.Equal(t, []byte("cert-pem"), certPEM)
		require.Nil(t, keyPEM)
	})

	t.Run("LoadCert with key", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		certPEM, keyPEM := ca.Get()

		certFile := t.TempDir() + "/cert.pem"
		keyFile := t.TempDir() + "/key.pem"
		require.NoError(t, os.WriteFile(certFile, certPEM, 0600))
		require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

		cert, err := LoadCert(certFile, keyFile)
		require.NoError(t, err)

		loadedCertPEM, loadedKeyPEM := cert.Get()
		require.Equal(t, certPEM, loadedCertPEM)
		require.Equal(t, keyPEM, loadedKeyPEM)

		tlsCert, err := cert.AsTLSCertificate()
		require.NoError(t, err)
		require.Equal(t, "Test CA", tlsCert.Leaf.Subject.CommonName)
	})

	t.Run("LoadCert without key", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		certPEM, _ := ca.Get()

		certFile := t.TempDir() + "/cert.pem"
		require.NoError(t, os.WriteFile(certFile, certPEM, 0600))

		cert, err := LoadCert(certFile, "")
		require.NoError(t, err)

		loadedCertPEM, loadedKeyPEM := cert.Get()
		require.Equal(t, certPEM, loadedCertPEM)
		require.Nil(t, loadedKeyPEM)
	})

	t.Run("LoadCert cert file not found", func(t *testing.T) {
		_, err := LoadCert("/nonexistent/cert.pem", "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "read cert")
	})

	t.Run("LoadCert key file not found", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		certPEM, _ := ca.Get()

		certFile := t.TempDir() + "/cert.pem"
		require.NoError(t, os.WriteFile(certFile, certPEM, 0600))

		_, err := LoadCert(certFile, "/nonexistent/key.pem")
		require.Error(t, err)
		require.Contains(t, err.Error(), "read key")
	})

	t.Run("Get", func(t *testing.T) {
		cert := &Cert{}
		cert.Set([]byte("cert-pem"), []byte("key-pem"))

		certPEM, keyPEM := cert.Get()
		require.Equal(t, []byte("cert-pem"), certPEM)
		require.Equal(t, []byte("key-pem"), keyPEM)
	})

	t.Run("AsTLSCertificate", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")

		tlsCert, err := ca.AsTLSCertificate()
		require.NoError(t, err)
		require.NotNil(t, tlsCert)
		require.NotNil(t, tlsCert.Leaf)
		require.Equal(t, "Test CA", tlsCert.Leaf.Subject.CommonName)
		require.NotNil(t, tlsCert.PrivateKey)
	})

	t.Run("AsTLSCertificate caches result", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")

		tlsCert1, err := ca.AsTLSCertificate()
		require.NoError(t, err)

		tlsCert2, err := ca.AsTLSCertificate()
		require.NoError(t, err)
		require.Same(t, tlsCert1, tlsCert2)
	})

	t.Run("AsTLSCertificate errors without key", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		certPEM, _ := ca.Get()

		cert := NewCert(certPEM, nil)

		_, err := cert.AsTLSCertificate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "find any PEM data in key input")
	})

	t.Run("AsTLSCertificate with certificate chain", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		leaf := generateTestLeaf(t, ca, "Leaf Cert")

		// Build chain: leaf + CA.
		leafPEM, leafKeyPEM := leaf.Get()
		caPEM, _ := ca.Get()
		chainPEM := append(leafPEM, caPEM...)

		cert := NewCert(chainPEM, leafKeyPEM)

		tlsCert, err := cert.AsTLSCertificate()
		require.NoError(t, err)
		require.Len(t, tlsCert.Certificate, 2)
		require.Equal(t, "Leaf Cert", tlsCert.Leaf.Subject.CommonName)
	})

	t.Run("AsLeafCertificate", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")

		leaf, key, err := ca.AsLeafCertificate()
		require.NoError(t, err)
		require.NotNil(t, leaf)
		require.Equal(t, "Test CA", leaf.Subject.CommonName)
		require.NotNil(t, key)
	})

	t.Run("AsLeafCertificate errors without key", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		certPEM, _ := ca.Get()

		cert := NewCert(certPEM, nil)

		_, _, err := cert.AsLeafCertificate()
		require.Error(t, err)
	})

	t.Run("Set clears cached forms", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")

		tlsCert1, err := ca.AsTLSCertificate()
		require.NoError(t, err)
		require.Equal(t, "Test CA", tlsCert1.Leaf.Subject.CommonName)

		newCA := generateTestCA(t, "New CA")
		newCertPEM, newKeyPEM := newCA.Get()
		ca.Set(newCertPEM, newKeyPEM)

		tlsCert2, err := ca.AsTLSCertificate()
		require.NoError(t, err)
		require.Equal(t, "New CA", tlsCert2.Leaf.Subject.CommonName)
		require.NotSame(t, tlsCert1, tlsCert2)
	})

	t.Run("AsCAPool with single cert", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		certPEM, _ := ca.Get()

		cert := NewCert(certPEM, nil)

		pool, err := cert.AsCAPool()
		require.NoError(t, err)
		require.NotNil(t, pool)

		// Verify the pool contains the cert by checking it can verify.
		tlsCert, err := ca.AsTLSCertificate()
		require.NoError(t, err)
		_, err = tlsCert.Leaf.Verify(x509.VerifyOptions{Roots: pool})
		require.NoError(t, err)
	})

	t.Run("AsCAPool with multiple certs", func(t *testing.T) {
		ca1 := generateTestCA(t, "CA One")
		ca2 := generateTestCA(t, "CA Two")
		certPEM1, _ := ca1.Get()
		certPEM2, _ := ca2.Get()

		// Concatenate both CA certs.
		combinedPEM := append(certPEM1, certPEM2...)

		cert := NewCert(combinedPEM, nil)

		pool, err := cert.AsCAPool()
		require.NoError(t, err)
		require.NotNil(t, pool)

		// Verify both CAs are in the pool.
		tlsCert1, err := ca1.AsTLSCertificate()
		require.NoError(t, err)
		tlsCert2, err := ca2.AsTLSCertificate()
		require.NoError(t, err)
		_, err = tlsCert1.Leaf.Verify(x509.VerifyOptions{Roots: pool})
		require.NoError(t, err)
		_, err = tlsCert2.Leaf.Verify(x509.VerifyOptions{Roots: pool})
		require.NoError(t, err)
	})

	t.Run("AsCAPool caches pool", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		certPEM, _ := ca.Get()

		cert := NewCert(certPEM, nil)

		pool1, err := cert.AsCAPool()
		require.NoError(t, err)

		pool2, err := cert.AsCAPool()
		require.NoError(t, err)
		require.Same(t, pool1, pool2)

		// Set clears cache, new pool is returned.
		cert.Set(certPEM, nil)
		pool3, err := cert.AsCAPool()
		require.NoError(t, err)
		require.NotSame(t, pool1, pool3)
	})
}
