// Copyright 2021 The Cockroach Authors.
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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// TODO(aaron-crl): This shared a name and purpose with the value in
// pkg/cli/cert.go and should be consolidated.
const defaultKeySize = 2048

// notBeforeMargin provides a window to compensate for potential clock skew.
const notBeforeMargin = time.Hour * 24

// createCertificateSerialNumber is a helper function that generates a
// random value between [1, 2^130). The use of crypto random for a serial with
// greater than 128 bits of entropy provides for a potential future where we
// decided to rely on the serial for security purposes.
func createCertificateSerialNumber() (serialNumber *big.Int, err error) {
	max := new(big.Int)
	max.Exp(big.NewInt(2), big.NewInt(130), nil).Sub(max, big.NewInt(1))

	// serialNumber is set using rand.Int which yields a value between [0, max)
	// where max is (2^130)-1.
	serialNumber, err = rand.Int(rand.Reader, max)
	if err != nil {
		err = errors.Wrap(err, "failed to create new serial number")
	}

	// We then add 1 to the result ensuring a range of [1,2^130).
	serialNumber.Add(serialNumber, big.NewInt(1))

	return
}

// LoggerFn is the type we use to inject logging functions into the
// security package to avoid circular dependencies.
type LoggerFn = func(ctx context.Context, format string, args ...interface{})

func describeCert(cert *x509.Certificate) redact.RedactableString {
	var buf redact.StringBuilder
	buf.SafeString("{\n")
	buf.Printf("  SN: %s,\n", cert.SerialNumber)
	buf.Printf("  CA: %v,\n", cert.IsCA)
	buf.Printf("  Issuer: %q,\n", cert.Issuer)
	buf.Printf("  Subject: %q,\n", cert.Subject)
	buf.Printf("  NotBefore: %s,\n", cert.NotBefore)
	buf.Printf("  NotAfter: %s", cert.NotAfter)
	buf.Printf(" (Validity: %s),\n", cert.NotAfter.Sub(timeutil.Now()))
	if !cert.IsCA {
		buf.Printf("  DNS: %v,\n", cert.DNSNames)
		buf.Printf("  IP: %v\n", cert.IPAddresses)
	}
	buf.SafeString("}")
	return buf.RedactableString()
}

const (
	crlOrg      = "Cockroach"
	crlIssuerOU = "automatic cert generator"
)

// CreateCACertAndKey will create a CA with a validity beginning
// now() and expiring after `lifespan`. This is a utility function to help
// with cluster auto certificate generation.
func CreateCACertAndKey(
	ctx context.Context, loggerFn LoggerFn, lifespan time.Duration, service string,
) (certPEM, keyPEM *pem.Block, err error) {
	notBefore := timeutil.Now().Add(-notBeforeMargin)
	notAfter := timeutil.Now().Add(lifespan)

	// Create random serial number for CA.
	serialNumber, err := createCertificateSerialNumber()
	if err != nil {
		return nil, nil, err
	}

	// Create short lived initial CA template.
	ca := &x509.Certificate{
		SerialNumber: serialNumber,
		Issuer: pkix.Name{
			Organization: []string{crlOrg},
			CommonName:   service,
		},
		Subject: pkix.Name{
			Organization: []string{crlOrg},
			CommonName:   service,
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageContentCommitment,
		BasicConstraintsValid: true,
		MaxPathLen:            1,
	}
	if loggerFn != nil {
		loggerFn(ctx, "creating CA cert from template: %s", describeCert(ca))
	}

	// Create private and public key for CA.
	caPrivKey, err := rsa.GenerateKey(rand.Reader, defaultKeySize)
	if err != nil {
		return nil, nil, err
	}

	caPrivKeyPEM, err := PrivateKeyToPEM(caPrivKey)
	if err != nil {
		return nil, nil, err
	}

	if loggerFn != nil {
		loggerFn(ctx, "signing CA cert")
	}
	// Create CA certificate then PEM encode it.
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, err
	}

	caPEM := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	}

	return caPEM, caPrivKeyPEM, nil
}

// CreateServiceCertAndKey creates a cert/key pair signed by the provided CA.
// This is a utility function to help with cluster auto certificate generation.
func CreateServiceCertAndKey(
	ctx context.Context,
	loggerFn LoggerFn,
	lifespan time.Duration,
	commonName string,
	hostnames []string,
	caCertBlock, caKeyBlock *pem.Block,
	serviceCertIsAlsoValidAsClient bool,
) (certPEM *pem.Block, keyPEM *pem.Block, err error) {
	notBefore := timeutil.Now().Add(-notBeforeMargin)
	notAfter := timeutil.Now().Add(lifespan)

	// Create random serial number for CA.
	serialNumber, err := createCertificateSerialNumber()
	if err != nil {
		return nil, nil, err
	}

	caCert, err := x509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		err = errors.Wrap(err, "failed to parse valid Certificate from PEM blob")
		return nil, nil, err
	}

	caKey, err := x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		err = errors.Wrap(err, "failed to parse valid Private Key from PEM blob")
		return nil, nil, err
	}

	// Bulid service certificate template; template will be used for all
	// autogenerated service certificates.
	// TODO(aaron-crl): This should match the implementation in
	// pkg/security/x509.go until we can consolidate them.
	serviceCert := &x509.Certificate{
		SerialNumber: serialNumber,
		Issuer: pkix.Name{
			Organization:       []string{crlOrg},
			OrganizationalUnit: []string{crlIssuerOU},
			CommonName:         caCommonName,
		},
		Subject: pkix.Name{
			Organization: []string{crlOrg},
			CommonName:   commonName,
		},
		NotBefore:   notBefore,
		NotAfter:    notAfter,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
	}

	if serviceCertIsAlsoValidAsClient {
		serviceCert.ExtKeyUsage = append(serviceCert.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	}

	// Attempt to parse hostname as IP, if successful add it as an IP
	// otherwise presume it is a DNS name.
	// TODO(aaron-crl): Pass these values via config object.
	for _, hostname := range hostnames {
		ip := net.ParseIP(hostname)
		if ip != nil {
			serviceCert.IPAddresses = []net.IP{ip}
		} else {
			serviceCert.DNSNames = []string{hostname}
		}
	}

	if loggerFn != nil {
		loggerFn(ctx, "creating service cert from template: %s", describeCert(serviceCert))
	}

	servicePrivKey, err := rsa.GenerateKey(rand.Reader, defaultKeySize)
	if err != nil {
		return nil, nil, err
	}

	if loggerFn != nil {
		loggerFn(ctx, "signing service cert")
	}
	serviceCertBytes, err := x509.CreateCertificate(rand.Reader, serviceCert, caCert, &servicePrivKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	serviceCertBlock := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serviceCertBytes,
	}

	servicePrivKeyPEM, err := PrivateKeyToPEM(servicePrivKey)
	if err != nil {
		return nil, nil, err
	}

	return serviceCertBlock, servicePrivKeyPEM, nil
}
