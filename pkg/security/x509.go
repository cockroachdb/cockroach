// Copyright 2015 The Cockroach Authors.
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
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Utility to generate x509 certificates, both CA and not.
// This is mostly based on http://golang.org/src/crypto/tls/generate_cert.go
// Most fields and settings are hard-coded. TODO(marc): allow customization.

const (
	// Make certs valid a day before to handle clock issues, specifically
	// boot2docker: https://github.com/boot2docker/boot2docker/issues/69
	validFrom     = -time.Hour * 24
	maxPathLength = 1
	caCommonName  = "Cockroach CA"
)

// newTemplate returns a partially-filled template.
// It should be further populated based on whether the cert is for a CA or node.
func newTemplate(commonName string, lifetime time.Duration) (*x509.Certificate, error) {
	// Generate a random serial number.
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, err
	}

	now := timeutil.Now()
	notBefore := now.Add(validFrom)
	notAfter := now.Add(lifetime)

	cert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Cockroach"},
			CommonName:   commonName,
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
	}

	return cert, nil
}

// GenerateCA generates a CA certificate and signs it using the signer (a private key).
// It returns the DER-encoded certificate.
func GenerateCA(signer crypto.Signer, lifetime time.Duration) ([]byte, error) {
	template, err := newTemplate(caCommonName, lifetime)
	if err != nil {
		return nil, err
	}

	// Set CA-specific fields.
	template.BasicConstraintsValid = true
	template.IsCA = true
	template.MaxPathLen = maxPathLength
	template.KeyUsage |= x509.KeyUsageCertSign
	template.KeyUsage |= x509.KeyUsageContentCommitment

	certBytes, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		signer.Public(),
		signer)
	if err != nil {
		return nil, err
	}

	return certBytes, nil
}

func checkLifetimeAgainstCA(cert, ca *x509.Certificate) error {
	if ca.NotAfter.After(cert.NotAfter) || ca.NotAfter.Equal(cert.NotAfter) {
		return nil
	}

	now := timeutil.Now()
	// Truncate the lifetime to round hours, the maximum "pretty" duration.
	niceCALifetime := ca.NotAfter.Sub(now).Hours()
	niceCertLifetime := cert.NotAfter.Sub(now).Hours()
	return errors.Errorf("CA lifetime is %fh, shorter than the requested %fh. "+
		"Renew CA certificate, or rerun with --lifetime=%dh for a shorter duration.",
		niceCALifetime, niceCertLifetime, int64(niceCALifetime))
}

// GenerateServerCert generates a server certificate and returns the cert bytes.
// Takes in the CA cert and private key, the node public key, the certificate
// lifetime, the list of hosts/ip addresses this certificate applies to, and at
// least one permitted key usage.
func GenerateServerCert(
	caCert *x509.Certificate,
	caPrivateKey crypto.PrivateKey,
	nodePublicKey crypto.PublicKey,
	lifetime time.Duration,
	user string,
	hosts []string,
	usage ...x509.ExtKeyUsage,
) ([]byte, error) {
	if len(usage) == 0 {
		return nil, errors.New("must specify at least one ExtKeyUsage")
	}
	// Create template for user.
	template, err := newTemplate(user, lifetime)
	if err != nil {
		return nil, err
	}

	// Don't issue certificates that outlast the CA cert.
	if err := checkLifetimeAgainstCA(template, caCert); err != nil {
		return nil, err
	}

	template.ExtKeyUsage = usage
	addHostsToTemplate(template, hosts)

	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, nodePublicKey, caPrivateKey)
	if err != nil {
		return nil, err
	}

	return certBytes, nil
}

func addHostsToTemplate(template *x509.Certificate, hosts []string) {
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}
}

// GenerateUIServerCert generates a server certificate for the Admin UI and returns the cert bytes.
// Takes in the CA cert and private key, the UI cert public key, the certificate lifetime,
// and the list of hosts/ip addresses this certificate applies to.
func GenerateUIServerCert(
	caCert *x509.Certificate,
	caPrivateKey crypto.PrivateKey,
	certPublicKey crypto.PublicKey,
	lifetime time.Duration,
	hosts []string,
) ([]byte, error) {
	// Use the first host as the CN. We still place all in the alternative subject name.
	template, err := newTemplate(hosts[0], lifetime)
	if err != nil {
		return nil, err
	}

	// Don't issue certificates that outlast the CA cert.
	if err := checkLifetimeAgainstCA(template, caCert); err != nil {
		return nil, err
	}

	// Only server authentication is allowed.
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	addHostsToTemplate(template, hosts)

	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, certPublicKey, caPrivateKey)
	if err != nil {
		return nil, err
	}

	return certBytes, nil
}

// GenerateClientCert generates a client certificate and returns the cert bytes.
// Takes in the CA cert and private key, the client public key, the certificate lifetime,
// and the username.
//
// This is used both for vanilla CockroachDB user client certs as well as for the
// multi-tenancy KV auth broker (in which case the user is a SQL tenant).
func GenerateClientCert(
	caCert *x509.Certificate,
	caPrivateKey crypto.PrivateKey,
	clientPublicKey crypto.PublicKey,
	lifetime time.Duration,
	user string,
) ([]byte, error) {

	// TODO(marc): should we add extra checks?
	if len(user) == 0 {
		return nil, errors.Errorf("user cannot be empty")
	}

	// Create template for user.
	template, err := newTemplate(user, lifetime)
	if err != nil {
		return nil, err
	}

	// Don't issue certificates that outlast the CA cert.
	if err := checkLifetimeAgainstCA(template, caCert); err != nil {
		return nil, err
	}

	// Set client-specific fields.
	// Client authentication only.
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}

	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, clientPublicKey, caPrivateKey)
	if err != nil {
		return nil, err
	}

	return certBytes, nil
}
