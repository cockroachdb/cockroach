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
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
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
		return nil, nil, errors.Wrapf(err, "error loading CA certificate %s and key %s",
			sslCA, sslCAKey)
	}

	// Extract x509 certificate from tls cert.
	x509Cert, err := x509.ParseCertificate(caCert.Certificate[0])
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error parsing CA certificate %s", sslCA)
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

func writePKCS8KeyToFile(keyFilePath string, key crypto.PrivateKey, overwrite bool) error {
	keyBytes, err := PrivateKeyToPKCS8(key)
	if err != nil {
		return err
	}

	return SafeWriteToFile(keyFilePath, keyFileMode, overwrite, keyBytes)
}

// CreateCAPair creates a general CA certificate and associated key.
func CreateCAPair(
	certsDir, caKeyPath string,
	keySize int,
	lifetime time.Duration,
	allowKeyReuse bool,
	overwrite bool,
) error {
	return createCACertAndKey(certsDir, caKeyPath, CAPem, keySize, lifetime, allowKeyReuse, overwrite)
}

// CreateTenantCAPair creates a tenant client CA pair. The private key is
// written to caKeyPath and the public key is created in certsDir.
func CreateTenantCAPair(
	certsDir, caKeyPath string,
	keySize int,
	lifetime time.Duration,
	allowKeyReuse bool,
	overwrite bool,
) error {
	return createCACertAndKey(certsDir, caKeyPath, TenantCAPem, keySize, lifetime, allowKeyReuse, overwrite)
}

// CreateClientCAPair creates a client CA certificate and associated key.
func CreateClientCAPair(
	certsDir, caKeyPath string,
	keySize int,
	lifetime time.Duration,
	allowKeyReuse bool,
	overwrite bool,
) error {
	return createCACertAndKey(certsDir, caKeyPath, ClientCAPem, keySize, lifetime, allowKeyReuse, overwrite)
}

// CreateUICAPair creates a UI CA certificate and associated key.
func CreateUICAPair(
	certsDir, caKeyPath string,
	keySize int,
	lifetime time.Duration,
	allowKeyReuse bool,
	overwrite bool,
) error {
	return createCACertAndKey(certsDir, caKeyPath, UICAPem, keySize, lifetime, allowKeyReuse, overwrite)
}

// createCACertAndKey creates a CA key and a CA certificate.
// If the certs directory does not exist, it is created.
// If the key does not exist, it is created.
// The certificate is written to the certs directory. If the file already exists,
// we append the original certificates to the new certificate.
//
// The filename of the certificate file must be specified.
// It should be one of:
// - ca.crt: the general CA certificate
// - ca-client.crt: the CA certificate to verify client certificates
func createCACertAndKey(
	certsDir, caKeyPath string,
	caType PemUsage,
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
	if caType != CAPem &&
		caType != TenantCAPem &&
		caType != ClientCAPem &&
		caType != UICAPem {

		return fmt.Errorf("caType argument to createCACertAndKey must be one of CAPem (%d), ClientCAPem (%d), or UICAPem (%d), got: %d",
			CAPem, ClientCAPem, UICAPem, caType)
	}

	// Create a certificate manager with "create dir if not exist".
	cm, err := NewCertificateManagerFirstRun(certsDir, CommandTLSSettings{})
	if err != nil {
		return err
	}

	var key crypto.PrivateKey
	if _, err := os.Stat(caKeyPath); err != nil {
		if !oserror.IsNotExist(err) {
			return errors.Wrapf(err, "could not stat CA key file %s", caKeyPath)
		}

		// The key does not exist: create it.
		key, err = rsa.GenerateKey(rand.Reader, keySize)
		if err != nil {
			return errors.Wrap(err, "could not generate new CA key")
		}

		// overwrite is not technically needed here, but use it in case something else created it.
		if err := writeKeyToFile(caKeyPath, key, overwrite); err != nil {
			return errors.Wrapf(err, "could not write CA key to file %s", caKeyPath)
		}

		log.Infof(context.Background(), "generated CA key %s", caKeyPath)
	} else {
		if !allowKeyReuse {
			return errors.Errorf("CA key %s exists, but key reuse is disabled", caKeyPath)
		}
		// The key exists, parse it.
		contents, err := os.ReadFile(caKeyPath)
		if err != nil {
			return errors.Wrapf(err, "could not read CA key file %s", caKeyPath)
		}

		key, err = PEMToPrivateKey(contents)
		if err != nil {
			return errors.Wrapf(err, "could not parse CA key file %s", caKeyPath)
		}

		log.Infof(context.Background(), "using CA key from file %s", caKeyPath)
	}

	// Generate certificate.
	certContents, err := GenerateCA(key.(crypto.Signer), lifetime)
	if err != nil {
		return errors.Wrap(err, "could not generate CA certificate")
	}

	var certPath string
	// We've already checked the caType value at the beginning of this function.
	switch caType {
	case CAPem:
		certPath = cm.CACertPath()
	case TenantCAPem:
		certPath = cm.TenantCACertPath()
	case ClientCAPem:
		certPath = cm.ClientCACertPath()
	case UICAPem:
		certPath = cm.UICACertPath()
	default:
		return errors.Newf("unknown CA type %v", caType)
	}

	var existingCertificates []*pem.Block
	if _, err := os.Stat(certPath); err == nil {
		// The cert file already exists, load certificates.
		contents, err := os.ReadFile(certPath)
		if err != nil {
			return errors.Wrapf(err, "could not read existing CA cert file %s", certPath)
		}

		existingCertificates, err = PEMToCertificates(contents)
		if err != nil {
			return errors.Wrapf(err, "could not parse existing CA cert file %s", certPath)
		}
		log.Infof(context.Background(), "found %d certificates in %s",
			len(existingCertificates), certPath)
	} else if !oserror.IsNotExist(err) {
		return errors.Wrapf(err, "could not stat CA cert file %s", certPath)
	}

	// Always place the new certificate first.
	certificates := []*pem.Block{{Type: "CERTIFICATE", Bytes: certContents}}
	certificates = append(certificates, existingCertificates...)

	if err := WritePEMToFile(certPath, certFileMode, overwrite, certificates...); err != nil {
		return errors.Wrapf(err, "could not write CA certificate file %s", certPath)
	}

	log.Infof(context.Background(), "wrote %d certificates to %s", len(certificates), certPath)

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

	// Create a certificate manager with "create dir if not exist".
	cm, err := NewCertificateManagerFirstRun(certsDir, CommandTLSSettings{})
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
		return errors.Wrap(err, "could not generate new node key")
	}

	// Allow control of the principal to place in the cert via an env var. This
	// is intended for testing purposes only.
	nodeUser, _ := username.MakeSQLUsernameFromUserInput(
		envutil.EnvOrDefaultString("COCKROACH_CERT_NODE_USER", username.NodeUser),
		username.PurposeValidation)

	nodeCert, err := GenerateServerCert(caCert, caPrivateKey,
		nodeKey.Public(), lifetime, nodeUser, hosts)
	if err != nil {
		return errors.Wrap(err, "error creating node server certificate and key")
	}

	certPath := cm.NodeCertPath()
	if err := writeCertificateToFile(certPath, nodeCert, overwrite); err != nil {
		return errors.Wrapf(err, "error writing node server certificate to %s", certPath)
	}
	log.Infof(context.Background(), "generated node certificate: %s", certPath)

	keyPath := cm.NodeKeyPath()
	if err := writeKeyToFile(keyPath, nodeKey, overwrite); err != nil {
		return errors.Wrapf(err, "error writing node server key to %s", keyPath)
	}
	log.Infof(context.Background(), "generated node key: %s", keyPath)

	return nil
}

// CreateUIPair creates a UI certificate and key using the UI CA.
// The CA cert and key must load properly. If multiple certificates
// exist in the CA cert, the first one is used.
func CreateUIPair(
	certsDir, caKeyPath string, keySize int, lifetime time.Duration, overwrite bool, hosts []string,
) error {
	if len(caKeyPath) == 0 {
		return errors.New("the path to the CA key is required")
	}
	if len(certsDir) == 0 {
		return errors.New("the path to the certs directory is required")
	}

	// Create a certificate manager with "create dir if not exist".
	cm, err := NewCertificateManagerFirstRun(certsDir, CommandTLSSettings{})
	if err != nil {
		return err
	}

	// Load the CA pair.
	caCert, caPrivateKey, err := loadCACertAndKey(cm.UICACertPath(), caKeyPath)
	if err != nil {
		return err
	}

	// Generate certificates and keys.
	uiKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return errors.Wrap(err, "could not generate new UI key")
	}

	uiCert, err := GenerateUIServerCert(caCert, caPrivateKey, uiKey.Public(), lifetime, hosts)
	if err != nil {
		return errors.Wrap(err, "error creating UI server certificate and key")
	}

	certPath := cm.UICertPath()
	if err := writeCertificateToFile(certPath, uiCert, overwrite); err != nil {
		return errors.Wrapf(err, "error writing UI server certificate to %s", certPath)
	}
	log.Infof(context.Background(), "generated UI certificate: %s", certPath)

	keyPath := cm.UIKeyPath()
	if err := writeKeyToFile(keyPath, uiKey, overwrite); err != nil {
		return errors.Wrapf(err, "error writing UI server key to %s", keyPath)
	}
	log.Infof(context.Background(), "generated UI key: %s", keyPath)

	return nil
}

// CreateClientPair creates a node key and certificate.
// The CA cert and key must load properly. If multiple certificates
// exist in the CA cert, the first one is used.
// If a client CA exists, this is used instead.
// If wantPKCS8Key is true, the private key in PKCS#8 encoding is written as well.
// tenantIDs indicates the tenant(s) the client certificate is being scoped to.
// By default, tenantID is set to the system tenant ID.
func CreateClientPair(
	certsDir, caKeyPath string,
	keySize int,
	lifetime time.Duration,
	overwrite bool,
	user username.SQLUsername,
	tenantIDs []roachpb.TenantID,
	wantPKCS8Key bool,
) error {
	if len(caKeyPath) == 0 {
		return errors.New("the path to the CA key is required")
	}
	if len(certsDir) == 0 {
		return errors.New("the path to the certs directory is required")
	}

	// Create a certificate manager with "create dir if not exist".
	cm, err := NewCertificateManagerFirstRun(certsDir, CommandTLSSettings{})
	if err != nil {
		return err
	}

	var caCertPath string
	// Check to see if we are using a client CA.
	// We only check for its presence, not whether it has errors.
	if cm.ClientCACert() != nil {
		caCertPath = cm.ClientCACertPath()
	} else {
		caCertPath = cm.CACertPath()
	}

	// Load the CA pair.
	caCert, caPrivateKey, err := loadCACertAndKey(caCertPath, caKeyPath)
	if err != nil {
		return err
	}

	// Generate certificates and keys.
	clientKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return errors.Wrap(err, "could not generate new client key")
	}

	clientCert, err := GenerateClientCert(caCert, caPrivateKey, clientKey.Public(), lifetime, user, tenantIDs)
	if err != nil {
		return errors.Wrap(err, "error creating client certificate and key")
	}

	certPath := cm.ClientCertPath(user)
	keyPath := cm.ClientKeyPath(user)

	if err := writeCertificateToFile(certPath, clientCert, overwrite); err != nil {
		return errors.Wrapf(err, "error writing client certificate to %s", certPath)
	}
	log.Infof(context.Background(), "generated client certificate: %s", certPath)

	if err := writeKeyToFile(keyPath, clientKey, overwrite); err != nil {
		return errors.Wrapf(err, "error writing client key to %s", keyPath)
	}
	log.Infof(context.Background(), "generated client key: %s", keyPath)

	if wantPKCS8Key {
		pkcs8KeyPath := keyPath + ".pk8"
		if err := writePKCS8KeyToFile(pkcs8KeyPath, clientKey, overwrite); err != nil {
			return errors.Wrapf(err, "error writing client PKCS8 key to %s", pkcs8KeyPath)
		}
		log.Infof(context.Background(), "generated PKCS8 client key: %s", pkcs8KeyPath)
	}

	return nil
}

// TenantPair are client certs for use with multi-tenancy.
type TenantPair struct {
	PrivateKey *rsa.PrivateKey
	Cert       []byte
}

// CreateTenantPair creates a key and certificate for use as client certs
// when communicating with the KV layer. The tenant CA cert and key must load
// properly. If multiple certificates exist in the CA cert, the first one is
// used.
//
// To write the returned TenantPair to disk, use WriteTenantPair.
func CreateTenantPair(
	certsDir, caKeyPath string,
	keySize int,
	lifetime time.Duration,
	tenantIdentifier uint64,
	hosts []string,
) (*TenantPair, error) {
	if len(caKeyPath) == 0 {
		return nil, errors.New("the path to the CA key is required")
	}
	if len(certsDir) == 0 {
		return nil, errors.New("the path to the certs directory is required")
	}

	// Create a certificate manager with "create dir if not exist".
	cm, err := NewCertificateManagerFirstRun(certsDir, CommandTLSSettings{})
	if err != nil {
		return nil, err
	}

	// Load the tenant client CA cert info. Note that this falls back to the regular client CA which in turn falls
	// back to the CA.
	clientCA, err := cm.getTenantCACertLocked()
	if err != nil {
		return nil, err
	}

	// Load the CA pair.
	caCert, caPrivateKey, err := loadCACertAndKey(filepath.Join(certsDir, clientCA.Filename), caKeyPath)
	if err != nil {
		return nil, err
	}

	// Generate certificates and keys.
	clientKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, errors.Wrap(err, "could not generate new tenant key")
	}

	clientCert, err := GenerateTenantCert(
		caCert, caPrivateKey, clientKey.Public(), lifetime, tenantIdentifier, hosts,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error creating tenant certificate and key")
	}
	return &TenantPair{
		PrivateKey: clientKey,
		Cert:       clientCert,
	}, nil
}

// WriteTenantPair writes a TenantPair into certsDir.
func WriteTenantPair(certsDir string, cp *TenantPair, overwrite bool) error {
	cm, err := NewCertificateManagerFirstRun(certsDir, CommandTLSSettings{})
	if err != nil {
		return err
	}
	cert, err := x509.ParseCertificate(cp.Cert)
	if err != nil {
		return err
	}
	tenantIdentifier := cert.Subject.CommonName
	certPath := cm.TenantCertPath(tenantIdentifier)
	if err := writeCertificateToFile(certPath, cp.Cert, overwrite); err != nil {
		return errors.Wrapf(err, "error writing tenant certificate to %s", certPath)
	}
	log.Infof(context.Background(), "wrote SQL tenant client certificate: %s", certPath)

	keyPath := cm.TenantKeyPath(tenantIdentifier)
	if err := writeKeyToFile(keyPath, cp.PrivateKey, overwrite); err != nil {
		return errors.Wrapf(err, "error writing tenant key to %s", keyPath)
	}
	log.Infof(context.Background(), "generated tenant key: %s", keyPath)
	return nil
}

// CreateTenantSigningPair creates a tenant signing pair. The private key and
// public key are both created in certsDir.
func CreateTenantSigningPair(
	certsDir string, lifetime time.Duration, overwrite bool, tenantID uint64,
) error {
	if len(certsDir) == 0 {
		return errors.New("the path to the certs directory is required")
	}
	if tenantID == 0 {
		return errors.Errorf("tenantId %d is invalid (requires != 0)", tenantID)
	}

	tenantIdentifier := fmt.Sprintf("%d", tenantID)

	// Create a certificate manager with "create dir if not exist".
	cm, err := NewCertificateManagerFirstRun(certsDir, CommandTLSSettings{})
	if err != nil {
		return err
	}

	signingKeyPath := cm.TenantSigningKeyPath(tenantIdentifier)
	signingCertPath := cm.TenantSigningCertPath(tenantIdentifier)
	var pubKey crypto.PublicKey
	var privKey crypto.PrivateKey
	pubKey, privKey, err = ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return errors.Wrap(err, "could not generate new tenant signing key")
	}

	if err := writeKeyToFile(signingKeyPath, privKey, overwrite); err != nil {
		return errors.Wrapf(err, "could not write tenant signing key to file %s", signingKeyPath)
	}

	log.Infof(context.Background(), "generated tenant signing key %s", signingKeyPath)

	// Generate certificate.
	certContents, err := GenerateTenantSigningCert(pubKey, privKey, lifetime, tenantID)
	if err != nil {
		return errors.Wrap(err, "could not generate tenant signing certificate")
	}

	certificates := []*pem.Block{{Type: "CERTIFICATE", Bytes: certContents}}

	if err := WritePEMToFile(signingCertPath, certFileMode, overwrite, certificates...); err != nil {
		return errors.Wrapf(err, "could not write tenant signing certificate file %s", signingCertPath)
	}

	log.Infof(context.Background(), "wrote certificate to %s", signingCertPath)

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

// AppendCertificatesToBlob adds the passed PEM encoded certificates to the existing
// byte slice containing PEM encoded certificates, ensuring that there is a newline
// separating the original byte slice and each subsequent certificate byte slices.
func AppendCertificatesToBlob(certBlob []byte, newCerts ...[]byte) []byte {
	return bytes.Join(
		append(
			[][]byte{certBlob},
			newCerts...,
		),
		[]byte{'\n'},
	)
}
