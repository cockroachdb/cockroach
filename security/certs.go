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
	"path"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util"
)

// writeCertificateAndKey takes a x509 certificate and key and writes
// them out to the individual files.
// The certificate is written to <prefix>.crt and the key to <prefix>.key.
// TODO(marc): figure out how to include the plaintext certificate in the .crt file.
func writeCertificateAndKey(ctx *server.Context, prefix string,
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
	certFilePath := path.Join(ctx.Certs, prefix+".crt")
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
	keyFilePath := path.Join(ctx.Certs, prefix+".key")
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
func RunCreateCACert(ctx *server.Context) error {
	if ctx.Certs == "" {
		return util.Errorf("no certs directory specified, use --certs")
	}

	// Make the directory first.
	err := os.MkdirAll(ctx.Certs, 0755)
	if err != nil {
		return util.Errorf("error creating certs directory %s: %s", ctx.Certs, err)
	}

	// Generate certificate.
	certificate, key, err := GenerateCA()
	if err != nil {
		return util.Errorf("error creating CA certificate and key: %s", err)
	}

	err = writeCertificateAndKey(ctx, "ca", certificate, key)
	return err
}

// RunCreateNodeCert is the entry-point from the command-line interface
// to generate node cert and key.
func RunCreateNodeCert(ctx *server.Context, hosts []string) error {
	if ctx.Certs == "" {
		return util.Errorf("no certs directory specified, use --certs")
	}
	if len(hosts) == 0 {
		return util.Errorf("no hosts specified. Need at least one")
	}

	// Load the CA certificate.
	caCertPath := path.Join(ctx.Certs, "ca.crt")
	caKeyPath := path.Join(ctx.Certs, "ca.key")
	// LoadX509KeyPair does a bunch of validation, including len(Certificates) != 0.
	caCert, err := tls.LoadX509KeyPair(caCertPath, caKeyPath)
	if err != nil {
		return util.Errorf("error loading CA certificate %s and key %s: %s",
			caCertPath, caKeyPath, err)
	}

	// Extract x509 certificate from tls cert.
	x509Cert, err := x509.ParseCertificate(caCert.Certificate[0])
	if err != nil {
		return util.Errorf("error parsing CA certificate %s: %s", caCertPath, err)
	}

	// Generate certificate.
	certificate, key, err := GenerateNodeCert(x509Cert, caCert.PrivateKey, hosts)
	if err != nil {
		return util.Errorf("error creating node certificate and key: %s", err)
	}

	err = writeCertificateAndKey(ctx, "node", certificate, key)
	return err
}
