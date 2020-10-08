// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security_test

import (
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestLoadTLSConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cm, err := security.NewCertificateManager(security.EmbeddedCertsDir, security.CommandTLSSettings{})
	require.NoError(t, err)
	config, err := cm.GetServerTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, config.GetConfigForClient)
	config, err = config.GetConfigForClient(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	if err != nil {
		t.Fatalf("Failed to load TLS config: %v", err)
	}

	if len(config.Certificates) != 1 {
		t.Fatalf("config.Certificates should have 1 cert; found %d", len(config.Certificates))
	}
	cert := config.Certificates[0]
	asn1Data := cert.Certificate[0] // TODO Check len()

	x509Cert, err := x509.ParseCertificate(asn1Data)
	if err != nil {
		t.Fatalf("Couldn't parse test cert: %v", err)
	}

	if err = verifyX509Cert(x509Cert, "localhost", config.RootCAs); err != nil {
		t.Errorf("Couldn't verify test cert against server CA: %v", err)
	}

	if err = verifyX509Cert(x509Cert, "localhost", config.ClientCAs); err != nil {
		t.Errorf("Couldn't verify test cert against client CA: %v", err)
	}

	if err = verifyX509Cert(x509Cert, "google.com", config.RootCAs); err == nil {
		t.Errorf("Verified test cert for wrong hostname")
	}
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
