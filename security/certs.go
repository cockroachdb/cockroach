// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package security

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"

	"github.com/cockroachdb/cockroach/util"
)

// loadCACertAndKey loads the certificate and key files,parses them,
// and returns the x509 certificate and private key.
func loadCACertAndKey(sslCA, sslCAKey string) (*x509.Certificate, crypto.PrivateKey, error) {
	// LoadX509KeyPair does a bunch of validation, including len(Certificates) != 0.
	caCert, err := tls.LoadX509KeyPair(sslCA, sslCAKey)
	if err != nil {
		return nil, nil, util.Errorf("error loading CA certificate %s and key %s: %s",
			sslCA, sslCAKey, err)
	}

	// Extract x509 certificate from tls cert.
	x509Cert, err := x509.ParseCertificate(caCert.Certificate[0])
	if err != nil {
		return nil, nil, util.Errorf("error parsing CA certificate %s: %s", sslCA, err)
	}
	return x509Cert, caCert.PrivateKey, nil
}

// writeCertificateAndKey takes a x509 certificate and key and writes
// them out to the individual files.
// TODO(marc): figure out how to include the plaintext certificate in the .crt file.
func writeCertificateAndKey(certFilePath, keyFilePath string,
	certificate []byte, key crypto.PrivateKey) error {
	// Get PEM blocks for certificate and private key.
	certBlock, err := certificatePEMBlock(certificate)
	if err != nil {
		return err
	}

	keyBlock, err := privateKeyPEMBlock(key)
	if err != nil {
		return err
	}

	// Write certificate to file.
	certFile, err := os.OpenFile(certFilePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return util.Errorf("error creating certificate: %s", err)
	}

	if err := pem.Encode(certFile, certBlock); err != nil {
		return util.Errorf("error encoding certificate: %s", err)
	}

	if err := certFile.Close(); err != nil {
		return util.Errorf("error closing file %s: %s", certFilePath, err)
	}

	// Write key to file.
	keyFile, err := os.OpenFile(keyFilePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return util.Errorf("error creating key: %s", err)
	}

	if err := pem.Encode(keyFile, keyBlock); err != nil {
		return util.Errorf("error encoding key: %s", err)
	}

	if err := keyFile.Close(); err != nil {
		return util.Errorf("error closing file %s: %s", keyFilePath, err)
	}

	return nil
}

// RunCreateCACert is the entry-point from the command-line interface
// to generate CA cert and key.
// Takes in:
// - sslCA: path to the CA certificate
// - sslCAKey: path to the CA key
func RunCreateCACert(sslCA, sslCAKey string, keySize int) error {
	// Generate certificate.
	certificate, key, err := GenerateCA(keySize)
	if err != nil {
		return util.Errorf("error creating CA certificate and key: %s", err)
	}

	return writeCertificateAndKey(sslCA, sslCAKey, certificate, key)
}

// RunCreateNodeCert is the entry-point from the command-line interface
// to generate node certs and keys:
// - sslCA: path to the CA certificate
// - sslCAKey: path to the CA key
// - sslCert: path to the node certificate
// - sslCertKey: path to the node key
func RunCreateNodeCert(sslCA, sslCAKey, sslCert, sslCertKey string, keySize int, hosts []string) error {
	if len(hosts) == 0 {
		return util.Errorf("no hosts specified. Need at least one")
	}

	caCert, caKey, err := loadCACertAndKey(sslCA, sslCAKey)
	if err != nil {
		return err
	}

	// Generate certificates and keys.
	serverCert, serverKey, err := GenerateServerCert(caCert, caKey, keySize, hosts)
	if err != nil {
		return util.Errorf("error creating node server certificate and key: %s", err)
	}

	// TODO(marc): we fail if files already exist. At this point, we're checking four
	// different files, and should really make this more atomic (or at least check for existence first).
	err = writeCertificateAndKey(sslCert, sslCertKey, serverCert, serverKey)
	if err != nil {
		return err
	}

	return nil
}

// RunCreateClientCert is the entry-point from the command-line interface
// to generate a client cert and key.
// - sslCA: path to the CA certificate
// - sslCAKey: path to the CA key
// - sslCert: path to the node certificate
// - sslCertKey: path to the node key
func RunCreateClientCert(sslCA, sslCAKey, sslCert, sslCertKey string, keySize int, username string) error {
	if len(username) == 0 {
		return util.Errorf("no username specified.")
	}

	caCert, caKey, err := loadCACertAndKey(sslCA, sslCAKey)
	if err != nil {
		return err
	}

	// Generate certificate.
	certificate, key, err := GenerateClientCert(caCert, caKey, keySize, username)
	if err != nil {
		return util.Errorf("error creating client certificate and key: %s", err)
	}

	return writeCertificateAndKey(sslCert, sslCertKey, certificate, key)
}
