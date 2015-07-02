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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package security

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/util"
)

const (
	serverCertFile = "node.server.crt"
	serverKeyFile  = "node.server.key"
)

func clientCertFile(username string) string {
	return username + ".client.crt"
}

func clientKeyFile(username string) string {
	return username + ".client.key"
}

// loadCACertAndKey loads the certificate and key files in the specified
// directory, parses them, and returns the x509 certificate and private key.
func loadCACertAndKey(certsDir string) (*x509.Certificate, crypto.PrivateKey, error) {
	// Load the CA certificate.
	caCertPath := filepath.Join(certsDir, "ca.crt")
	caKeyPath := filepath.Join(certsDir, "ca.key")

	// LoadX509KeyPair does a bunch of validation, including len(Certificates) != 0.
	caCert, err := tls.LoadX509KeyPair(caCertPath, caKeyPath)
	if err != nil {
		return nil, nil, util.Errorf("error loading CA certificate %s and key %s: %s",
			caCertPath, caKeyPath, err)
	}

	// Extract x509 certificate from tls cert.
	x509Cert, err := x509.ParseCertificate(caCert.Certificate[0])
	if err != nil {
		return nil, nil, util.Errorf("error parsing CA certificate %s: %s", caCertPath, err)
	}
	return x509Cert, caCert.PrivateKey, nil
}

// writeCertificateAndKey takes a x509 certificate and key and writes
// them out to the individual files.
// The certificate is written to <prefix>.crt and the key to <prefix>.key.
// TODO(marc): figure out how to include the plaintext certificate in the .crt file.
func writeCertificateAndKey(certsDir string, prefix string,
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
	certFilePath := filepath.Join(certsDir, prefix+".crt")
	certFile, err := os.OpenFile(certFilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return util.Errorf("error creating certificate file %s: %s", certFilePath, err)
	}

	err = pem.Encode(certFile, certBlock)
	if err != nil {
		return util.Errorf("error encoding certificate: %s", err)
	}

	err = certFile.Close()
	if err != nil {
		return util.Errorf("error closing file %s: %s", certFilePath, err)
	}

	// Write key to file.
	keyFilePath := filepath.Join(certsDir, prefix+".key")
	keyFile, err := os.OpenFile(keyFilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return util.Errorf("error create key file %s: %s", keyFilePath, err)
	}

	err = pem.Encode(keyFile, keyBlock)
	if err != nil {
		return util.Errorf("error encoding key: %s", err)
	}

	err = keyFile.Close()
	if err != nil {
		return util.Errorf("error closing file %s: %s", keyFilePath, err)
	}

	return nil
}

// RunCreateCACert is the entry-point from the command-line interface
// to generate CA cert and key.
func RunCreateCACert(certsDir string, keySize int) error {
	if certsDir == "" {
		return util.Errorf("no certs directory specified, use --certs")
	}

	// Make the directory first.
	err := os.MkdirAll(certsDir, 0755)
	if err != nil {
		return util.Errorf("error creating certs directory %s: %s", certsDir, err)
	}

	// Generate certificate.
	certificate, key, err := GenerateCA(keySize)
	if err != nil {
		return util.Errorf("error creating CA certificate and key: %s", err)
	}

	return writeCertificateAndKey(certsDir, "ca", certificate, key)
}

// RunCreateNodeCert is the entry-point from the command-line interface
// to generate node certs and keys:
// - node.server.{crt,key}: server cert with list of dns/ip addresses
// - node.client.{crt,key}: client cert with "node" as the Common Name.
// We intentionally generate distinct keys for each cert.
func RunCreateNodeCert(certsDir string, keySize int, hosts []string) error {
	if certsDir == "" {
		return util.Errorf("no certs directory specified, use --certs")
	}
	if len(hosts) == 0 {
		return util.Errorf("no hosts specified. Need at least one")
	}

	caCert, caKey, err := loadCACertAndKey(certsDir)
	if err != nil {
		return err
	}

	// Generate certificates and keys.
	serverCert, serverKey, err := GenerateServerCert(caCert, caKey, keySize, hosts)
	if err != nil {
		return util.Errorf("error creating node server certificate and key: %s", err)
	}
	clientCert, clientKey, err := GenerateClientCert(caCert, caKey, keySize, NodeUser)
	if err != nil {
		return util.Errorf("error creating node client certificate and key: %s", err)
	}

	// TODO(marc): we fail if files already exist. At this point, we're checking four
	// different files, and should really make this more atomic (or at least check for existence first).
	err = writeCertificateAndKey(certsDir, NodeUser+".server", serverCert, serverKey)
	if err != nil {
		return err
	}
	return writeCertificateAndKey(certsDir, NodeUser+".client", clientCert, clientKey)
}

// RunCreateClientCert is the entry-point from the command-line interface
// to generate a client cert and key.
func RunCreateClientCert(certsDir string, keySize int, username string) error {
	if certsDir == "" {
		return util.Errorf("no certs directory specified, use --certs")
	}
	if len(username) == 0 {
		return util.Errorf("no username specified.")
	}

	caCert, caKey, err := loadCACertAndKey(certsDir)
	if err != nil {
		return err
	}

	// Generate certificate.
	certificate, key, err := GenerateClientCert(caCert, caKey, keySize, username)
	if err != nil {
		return util.Errorf("error creating client certificate and key: %s", err)
	}

	return writeCertificateAndKey(certsDir, username+".client", certificate, key)
}
