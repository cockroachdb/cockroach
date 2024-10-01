// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security_test

import (
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestLoadTLSConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cm, err := security.NewCertificateManager(certnames.EmbeddedCertsDir, security.CommandTLSSettings{})
	require.NoError(t, err)
	// We check that the UI Server tls.Config doesn't include the old cipher suites by default.
	config, err := cm.GetUIServerTLSConfig()
	require.NoError(t, err)
	clientConfig, err := config.GetConfigForClient(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, config.GetConfigForClient)
	require.NotNil(t, clientConfig)
	require.Equal(t, security.RecommendedCipherSuites(), clientConfig.CipherSuites)

	// Next we check that the Server tls.Config is correctly generated.
	config, err = cm.GetServerTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, config.GetConfigForClient)

	clientConfig, err = config.GetConfigForClient(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, clientConfig)
	require.Equal(t, security.RecommendedCipherSuites(), clientConfig.CipherSuites)
	if len(clientConfig.Certificates) != 1 {
		t.Fatalf("clientConfig.Certificates should have 1 cert; found %d", len(clientConfig.Certificates))
	}
	cert := clientConfig.Certificates[0]
	asn1Data := cert.Certificate[0] // TODO Check len()

	x509Cert, err := x509.ParseCertificate(asn1Data)
	if err != nil {
		t.Fatalf("Couldn't parse test cert: %v", err)
	}

	if err = verifyX509Cert(x509Cert, "localhost", clientConfig.RootCAs); err != nil {
		t.Errorf("Couldn't verify test cert against server CA: %v", err)
	}

	if err = verifyX509Cert(x509Cert, "localhost", clientConfig.ClientCAs); err != nil {
		t.Errorf("Couldn't verify test cert against client CA: %v", err)
	}

	if err = verifyX509Cert(x509Cert, "google.com", clientConfig.RootCAs); err == nil {
		t.Errorf("Verified test cert for wrong hostname")
	}
}

func TestLoadTLSConfigWithOldCipherSuites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reset := envutil.TestSetEnv(t, security.OldCipherSuitesEnabledEnv, "true")
	defer reset()

	cm, err := security.NewCertificateManager(certnames.EmbeddedCertsDir, security.CommandTLSSettings{})
	require.NoError(t, err)

	recommendedAndOldCipherSuites := append(
		security.RecommendedCipherSuites(),
		security.OldCipherSuites()...,
	)

	// We check that the UI Server tls.Config now includes the old cipher suites after the existing cipher suites.
	config, err := cm.GetUIServerTLSConfig()
	require.NoError(t, err)
	clientConfig, err := config.GetConfigForClient(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, clientConfig)
	require.Equal(t, recommendedAndOldCipherSuites, clientConfig.CipherSuites)

	// We check that the Server tls.Config now includes the old cipher suites after the existing cipher suites.
	config, err = cm.GetServerTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, config.GetConfigForClient)
	clientConfig, err = config.GetConfigForClient(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, clientConfig)
	require.Equal(t, recommendedAndOldCipherSuites, clientConfig.CipherSuites)
}

func verifyX509Cert(cert *x509.Certificate, dnsName string, roots *x509.CertPool) error {
	verifyOptions := x509.VerifyOptions{
		DNSName: dnsName,
		Roots:   roots,
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
	}
	_, err := cert.Verify(verifyOptions)
	return err
}
