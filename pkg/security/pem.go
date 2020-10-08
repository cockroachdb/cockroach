// Copyright 2017 The Cockroach Authors.
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
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
)

// WritePEMToFile writes an arbitrary number of PEM blocks to a file.
// The file "path" is created with "mode" and WRONLY|CREATE.
// If overwrite is true, the file will be overwritten if it exists.
func WritePEMToFile(path string, mode os.FileMode, overwrite bool, blocks ...*pem.Block) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if !overwrite {
		flags |= os.O_EXCL
	}
	f, err := os.OpenFile(path, flags, mode)
	if err != nil {
		return err
	}

	for _, p := range blocks {
		if err := pem.Encode(f, p); err != nil {
			return errors.Errorf("could not encode PEM block: %v", err)
		}
	}

	return f.Close()
}

// SafeWriteToFile writes the passed-in bytes to a file.
// The file "path" is created with "mode" and WRONLY|CREATE.
// If overwrite is true, the file will be overwritten if it exists.
func SafeWriteToFile(path string, mode os.FileMode, overwrite bool, contents []byte) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if !overwrite {
		flags |= os.O_EXCL
	}
	f, err := os.OpenFile(path, flags, mode)
	if err != nil {
		return err
	}

	n, err := f.Write(contents)
	if err == nil && n < len(contents) {
		err = io.ErrShortWrite
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

// PrivateKeyToPEM generates a PEM block from a private key.
func PrivateKeyToPEM(key crypto.PrivateKey) (*pem.Block, error) {
	switch k := key.(type) {
	case *rsa.PrivateKey:
		return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}, nil
	case *ecdsa.PrivateKey:
		bytes, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return nil, errors.Errorf("error marshaling ECDSA key: %s", err)
		}
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: bytes}, nil
	default:
		return nil, errors.Errorf("unknown key type: %v", k)
	}
}

// PrivateKeyToPKCS8 encodes a private key into PKCS#8.
func PrivateKeyToPKCS8(key crypto.PrivateKey) ([]byte, error) {
	return x509.MarshalPKCS8PrivateKey(key)
}

// PEMToCertificates parses multiple certificate PEM blocks and returns them.
// Each block must be a certificate.
// It is allowed to have zero certificates.
func PEMToCertificates(contents []byte) ([]*pem.Block, error) {
	certs := make([]*pem.Block, 0)
	for {
		var block *pem.Block
		block, contents = pem.Decode(contents)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return nil, errors.Errorf("block #%d is of type %s, not CERTIFICATE", len(certs), block.Type)
		}

		certs = append(certs, block)
	}

	return certs, nil
}

// PEMToPrivateKey parses a PEM block and returns the private key.
func PEMToPrivateKey(contents []byte) (crypto.PrivateKey, error) {
	keyBlock, remaining := pem.Decode(contents)
	if keyBlock == nil {
		return nil, errors.New("no PEM data found")
	}
	if len(remaining) != 0 {
		return nil, errors.New("more than one PEM block found")
	}
	if keyBlock.Type != "PRIVATE KEY" && !strings.HasSuffix(keyBlock.Type, " PRIVATE KEY") {
		return nil, errors.Errorf("PEM block is of type %s", keyBlock.Type)
	}

	return parsePrivateKey(keyBlock.Bytes)
}

// Taken straight from: golang.org/src/crypto/tls/tls.go
// Attempt to parse the given private key DER block. OpenSSL 0.9.8 generates
// PKCS#1 private keys by default, while OpenSSL 1.0.0 generates PKCS#8 keys.
// OpenSSL ecparam generates SEC1 EC private keys for ECDSA. We try all three.
func parsePrivateKey(der []byte) (crypto.PrivateKey, error) {
	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		switch key := key.(type) {
		case *rsa.PrivateKey, *ecdsa.PrivateKey:
			return key, nil
		default:
			return nil, errors.New("found unknown private key type in PKCS#8 wrapping")
		}
	}
	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key, nil
	}

	return nil, errors.New("failed to parse private key")
}
