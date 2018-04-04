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

package security

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

const (
	keyFileMode  = 0600
	certFileMode = 0644
)

// loadCACertAndKey loads the certificate and key files,parses them,
// and returns the x509 certificate and private key.
func loadCACertAndKey(sslCA, sslCAKey string) (*x509.Certificate, crypto.PrivateKey, error) {
	// LoadX509KeyPair does a bunch of validation, including len(Certificates) != 0.
	caCert, err := tls.LoadX509KeyPair(sslCA, sslCAKey)
	if err != nil {
		return nil, nil, errors.Errorf("error loading CA certificate %s and key %s: %s",
			sslCA, sslCAKey, err)
	}

	// Extract x509 certificate from tls cert.
	x509Cert, err := x509.ParseCertificate(caCert.Certificate[0])
	if err != nil {
		return nil, nil, errors.Errorf("error parsing CA certificate %s: %s", sslCA, err)
	}
	return x509Cert, caCert.PrivateKey, nil
}

func writeCertificateToFile(certFilePath string, certificate []byte, overwrite bool) error {
	certBlock := &pem.Block{Type: "CERTIFICATE", Bytes: certificate}

	return WritePEMToFile(certFilePath, certFileMode, overwrite, certBlock)
}

func writeKeyToFile(keyFilePath string, key crypto.PrivateKey, overwrite bool) error {
	keyBlock, err := PrivateKeyToPEM(key)
	if err != nil {
		return err
	}

	return WritePEMToFile(keyFilePath, keyFileMode, overwrite, keyBlock)
}

// CreateCAPair creates a CA key and a CA certificate.
// If the certs directory does not exist, it is created.
// If the key does not exist, it is created.
// The certificate is written to the certs directory. If the file already exists,
// we append the original certificates to the new certificate.
func CreateCAPair(
	certsDir, caKeyPath string,
	keySize int,
	lifetime time.Duration,
	allowKeyReuse bool,
	overwrite bool,
) error {
	if len(caKeyPath) == 0 {
		return errors.New("the path to the CA key is required")
	}
	if len(certsDir) == 0 {
		return errors.New("the path to the certs directory is required")
	}

	// The certificate manager expands the env for the certs directory.
	// For consistency, we need to do this for the key as well.
	caKeyPath = os.ExpandEnv(caKeyPath)

	// Create a certificate manager with "create dir if not exist".
	cm, err := NewCertificateManagerFirstRun(certsDir)
	if err != nil {
		return err
	}

	var key crypto.PrivateKey
	if _, err := os.Stat(caKeyPath); err != nil {
		if !os.IsNotExist(err) {
			return errors.Errorf("could not stat CA key file %s: %v", caKeyPath, err)
		}

		// The key does not exist: create it.
		key, err = rsa.GenerateKey(rand.Reader, keySize)
		if err != nil {
			return errors.Errorf("could not generate new CA key: %v", err)
		}

		// overwrite is not technically needed here, but use it in case something else created it.
		if err := writeKeyToFile(caKeyPath, key, overwrite); err != nil {
			return errors.Errorf("could not write CA key to file %s: %v", caKeyPath, err)
		}

		log.Infof(context.Background(), "Generated CA key %s", caKeyPath)
	} else {
		if !allowKeyReuse {
			return errors.Errorf("CA key %s exists, but key reuse is disabled", caKeyPath)
		}
		// The key exists, parse it.
		contents, err := ioutil.ReadFile(caKeyPath)
		if err != nil {
			return errors.Errorf("could not read CA key file %s: %v", caKeyPath, err)
		}

		key, err = PEMToPrivateKey(contents)
		if err != nil {
			return errors.Errorf("could not parse CA key file %s: %v", caKeyPath, err)
		}

		log.Infof(context.Background(), "Using CA key from file %s", caKeyPath)
	}

	// Generate certificate.
	certContents, err := GenerateCA(key.(crypto.Signer), lifetime)
	if err != nil {
		return errors.Errorf("could not generate CA certificate: %v", err)
	}

	certPath := cm.CACertPath()

	var existingCertificates []*pem.Block
	if _, err := os.Stat(certPath); err == nil {
		// The cert file already exists, load certificates.
		contents, err := ioutil.ReadFile(certPath)
		if err != nil {
			return errors.Errorf("could not read existing CA cert file %s: %v", certPath, err)
		}

		existingCertificates, err = PEMToCertificates(contents)
		if err != nil {
			return errors.Errorf("could not parse existing CA cert file %s: %v", certPath, err)
		}
		log.Infof(context.Background(), "Found %d certificates in %s",
			len(existingCertificates), certPath)
	} else if !os.IsNotExist(err) {
		return errors.Errorf("could not stat CA cert file %s: %v", certPath, err)
	}

	// Always place the new certificate first.
	certificates := []*pem.Block{{Type: "CERTIFICATE", Bytes: certContents}}
	certificates = append(certificates, existingCertificates...)

	if err := WritePEMToFile(certPath, certFileMode, overwrite, certificates...); err != nil {
		return errors.Errorf("could not write CA certificate file %s: %v", certPath, err)
	}

	log.Infof(context.Background(), "Wrote %d certificates to %s", len(certificates), certPath)

	return nil
}

// CreateNodePair creates a node key and certificate.
// The CA cert and key must load properly. If multiple certificates
// exist in the CA cert, the first one is used.
func CreateNodePair(
	certsDir, caKeyPath string, keySize int, lifetime time.Duration, overwrite bool, hosts []string,
) error {
	if len(caKeyPath) == 0 {
		return errors.New("the path to the CA key is required")
	}
	if len(certsDir) == 0 {
		return errors.New("the path to the certs directory is required")
	}

	// The certificate manager expands the env for the certs directory.
	// For consistency, we need to do this for the key as well.
	caKeyPath = os.ExpandEnv(caKeyPath)

	// Create a certificate manager with "create dir if not exist".
	cm, err := NewCertificateManagerFirstRun(certsDir)
	if err != nil {
		return err
	}

	// Load the CA pair.
	caCert, caPrivateKey, err := loadCACertAndKey(cm.CACertPath(), caKeyPath)
	if err != nil {
		return err
	}

	// Generate certificates and keys.
	nodeKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return errors.Errorf("could not generate new node key: %v", err)
	}

	nodeCert, err := GenerateServerCert(caCert, caPrivateKey, nodeKey.Public(), lifetime, hosts)
	if err != nil {
		return errors.Errorf("error creating node server certificate and key: %s", err)
	}

	certPath := cm.NodeCertPath()
	if err := writeCertificateToFile(certPath, nodeCert, overwrite); err != nil {
		return errors.Errorf("error writing node server certificate to %s: %v", certPath, err)
	}
	log.Infof(context.Background(), "Generated node certificate: %s", certPath)

	keyPath := cm.NodeKeyPath()
	if err := writeKeyToFile(keyPath, nodeKey, overwrite); err != nil {
		return errors.Errorf("error writing node server key to %s: %v", keyPath, err)
	}
	log.Infof(context.Background(), "Generated node key: %s", keyPath)

	return nil
}

// CreateClientPair creates a node key and certificate.
// The CA cert and key must load properly. If multiple certificates
// exist in the CA cert, the first one is used.
func CreateClientPair(
	certsDir, caKeyPath string, keySize int, lifetime time.Duration, overwrite bool, user string,
) error {
	if len(caKeyPath) == 0 {
		return errors.New("the path to the CA key is required")
	}
	if len(certsDir) == 0 {
		return errors.New("the path to the certs directory is required")
	}

	// The certificate manager expands the env for the certs directory.
	// For consistency, we need to do this for the key as well.
	caKeyPath = os.ExpandEnv(caKeyPath)

	// Create a certificate manager with "create dir if not exist".
	cm, err := NewCertificateManagerFirstRun(certsDir)
	if err != nil {
		return err
	}

	// Load the CA pair.
	caCert, caPrivateKey, err := loadCACertAndKey(cm.CACertPath(), caKeyPath)
	if err != nil {
		return err
	}

	// Generate certificates and keys.
	clientKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return errors.Errorf("could not generate new client key: %v", err)
	}

	clientCert, err := GenerateClientCert(caCert, caPrivateKey, clientKey.Public(), lifetime, user)
	if err != nil {
		return errors.Errorf("error creating client certificate and key: %s", err)
	}

	certPath := cm.ClientCertPath(user)
	if err := writeCertificateToFile(certPath, clientCert, overwrite); err != nil {
		return errors.Errorf("error writing client certificate to %s: %v", certPath, err)
	}
	log.Infof(context.Background(), "Generated client certificate: %s", certPath)

	keyPath := cm.ClientKeyPath(user)
	if err := writeKeyToFile(keyPath, clientKey, overwrite); err != nil {
		return errors.Errorf("error writing client key to %s: %v", keyPath, err)
	}
	log.Infof(context.Background(), "Generated client key: %s", keyPath)

	return nil
}

// PEMContentsToX509 takes raw pem-encoded contents and attempts to parse into
// x509.Certificate objects.
func PEMContentsToX509(contents []byte) ([]*x509.Certificate, error) {
	derCerts, err := PEMToCertificates(contents)
	if err != nil {
		return nil, err
	}

	certs := make([]*x509.Certificate, len(derCerts))
	for i, c := range derCerts {
		x509Cert, err := x509.ParseCertificate(c.Bytes)
		if err != nil {
			return nil, err
		}

		certs[i] = x509Cert
	}

	return certs, nil
}
