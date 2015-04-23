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

import (
	"crypto/x509"
	"testing"
)

func TestLoadTLSConfig(t *testing.T) {
	wrapperConfig, err := LoadTLSConfigFromDir("test_certs")
	if err != nil {
		t.Fatalf("Failed to load TLS config: %v", err)
	}

	config := wrapperConfig.config
	if config == nil {
		t.Fatalf("TLS config unexpectedly nil")
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
