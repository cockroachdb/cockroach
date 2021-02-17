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
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"log"
	"math/big"
	"time"
)

// CreateCACertAndKey will create a CA with a validity beginning
// now() and expiring after `lifespan`. This is a utility function to help
// with cluster auto certificate generation.
func CreateCACertAndKey(lifespan time.Duration, service string) (certPEM []byte, keyPEM []byte, err error) {
	// This function creates a short lived initial CA for node initialization

	notBefore := time.Now()
	notAfter := time.Now().Add(lifespan)

	// create random serial number for CA
	max := new(big.Int)
	max.Exp(big.NewInt(2), big.NewInt(130), nil).Sub(max, big.NewInt(1))
	serialNumber, err := rand.Int(rand.Reader, max)
	if nil != err {
		log.Fatal("Failed to create random serial number")
	}

	// Create short lived initial CA template
	ca := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:       []string{"Cockroach Labs"},
			OrganizationalUnit: []string{service},
			Country:            []string{"US"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// create private and public key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return
	}

	caPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	})

	// create CA certificate
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return
	}

	// pem encode it
	caPEM := new(bytes.Buffer)
	pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})

	certPEM = caPEM.Bytes()
	keyPEM = caPrivKeyPEM.Bytes()

	return
}

// CreateServiceCertAndKey creates a cert/key pair signed by the provided CA.
// This is a utility function to help with cluster auto certificate generation
func CreateServiceCertAndKey(lifespan time.Duration, service, hostname string, caCertPEM []byte, caKeyPEM []byte) (certPEM []byte, keyPEM []byte, err error) {

	// compute validity
	notBefore := time.Now()
	notAfter := time.Now().Add(lifespan)

	// create random serial number for CA
	max := new(big.Int)
	max.Exp(big.NewInt(2), big.NewInt(130), nil).Sub(max, big.NewInt(1))
	serialNumber, err := rand.Int(rand.Reader, max)
	if nil != err {
		log.Fatal("Failed to create random serial number")
	}

	caCertBlock, _ := pem.Decode(caCertPEM)
	if caCertBlock == nil {
		err = errors.New("failed to parse valid PEM from CaCertificate blob")
		return
	}

	caCert, err := x509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		err = errors.New("failed to parse valid Certificate from PEM blob")
		return
	}

	caKeyBlock, _ := pem.Decode(caKeyPEM)
	if caKeyBlock == nil {
		err = errors.New("failed to parse valid PEM from CaKey blob")
		return
	}

	caKey, err := x509.ParsePKCS8PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		err = errors.New("failed to parse valid Certificate from PEM blob")
		return
	}

	// bulid service template
	serviceCert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:       []string{"Cockroach Labs"},
			OrganizationalUnit: []string{service},
			Country:            []string{"US"},
		},
		DNSNames:    []string{hostname},
		NotBefore:   notBefore,
		NotAfter:    notAfter,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature,
	}

	servicePrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return
	}

	serviceCertBytes, err := x509.CreateCertificate(rand.Reader, serviceCert, caCert, &servicePrivKey.PublicKey, caKey)
	if err != nil {
		return
	}

	serviceCertBlock := new(bytes.Buffer)
	pem.Encode(serviceCertBlock, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serviceCertBytes,
	})

	certPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(certPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(servicePrivKey),
	})

	return
}
