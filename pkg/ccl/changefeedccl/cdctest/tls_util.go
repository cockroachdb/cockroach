// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const certLifetime = 30 * 24 * time.Hour

// EncodeBase64ToString stores the base64 encoding of src in dest
func EncodeBase64ToString(src []byte, dest *string) {
	if src != nil {
		encoded := base64.StdEncoding.EncodeToString(src)
		*dest = encoded
	}
}

// NewCACertBase64Encoded generates a new CA cert and returns the
// cert object as well as a base 64 encoded PEM version.
func NewCACertBase64Encoded() (*tls.Certificate, string, error) {
	keyLength := 2048

	caKey, err := rsa.GenerateKey(rand.Reader, keyLength)
	if err != nil {
		return nil, "", errors.Wrap(err, "CA private key")
	}

	caCert, _, err := GenerateCACert(caKey)
	if err != nil {
		return nil, "", errors.Wrap(err, "CA cert gen")
	}

	caKeyPEM, err := PemEncodePrivateKey(caKey)
	if err != nil {
		return nil, "", errors.Wrap(err, "pem encode CA key")
	}

	caCertPEM, err := PemEncodeCert(caCert)
	if err != nil {
		return nil, "", errors.Wrap(err, "pem encode CA cert")
	}

	cert, err := tls.X509KeyPair([]byte(caCertPEM), []byte(caKeyPEM))
	if err != nil {
		return nil, "", errors.Wrap(err, "CA cert parse from PEM")
	}

	var caCertBase64 string
	EncodeBase64ToString([]byte(caCertPEM), &caCertBase64)

	return &cert, caCertBase64, nil
}

// GenerateCACert generates a new self-signed CA cert using priv
func GenerateCACert(priv *rsa.PrivateKey) ([]byte, *x509.Certificate, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}

	certSpec := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Country:            []string{"US"},
			Organization:       []string{"Cockroach Labs"},
			OrganizationalUnit: []string{"Engineering"},
			CommonName:         "Roachtest Temporary Insecure CA",
		},
		NotBefore:             timeutil.Now(),
		NotAfter:              timeutil.Now().Add(certLifetime),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	cert, err := x509.CreateCertificate(rand.Reader, certSpec, certSpec, &priv.PublicKey, priv)
	return cert, certSpec, err
}

func pemEncode(dataType string, data []byte) (string, error) {
	ret := new(strings.Builder)
	err := pem.Encode(ret, &pem.Block{Type: dataType, Bytes: data})
	if err != nil {
		return "", err
	}

	return ret.String(), nil
}

// PemEncodePrivateKey encodes key in PEM format
func PemEncodePrivateKey(key *rsa.PrivateKey) (string, error) {
	return pemEncode("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
}

// PemEncodeCert encodes cert in PEM format
func PemEncodeCert(cert []byte) (string, error) {
	return pemEncode("CERTIFICATE", cert)
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	ret, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, errors.Wrap(err, "generate random serial")
	}
	return ret, nil
}
