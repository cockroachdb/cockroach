// Copyright 2017 The Cockroach Authors.
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
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"strings"

	"github.com/pkg/errors"
)

// WritePEMToFile writes an arbitrary number of PEM blocks to a file.
// The file "path" is created with "mode" and WRONLY|CREATE.
// If overwrite is true, the file will be overwritten if it exists.
func WritePEMToFile(path string, mode os.FileMode, overwrite bool, blocks ...*pem.Block) error {
	flags := os.O_WRONLY | os.O_CREATE
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

// PrivateKeyToPEM generates a PEM block from a private key.
func PrivateKeyToPEM(key crypto.PrivateKey) (*pem.Block, error) {
	switch k := key.(type) {
	case *rsa.PrivateKey:
		return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}, nil
	case *ecdsa.PrivateKey:
		bytes, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return nil, errors.Errorf("error marshalling ECDSA key: %s", err)
		}
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: bytes}, nil
	default:
		return nil, errors.Errorf("unknown key type: %v", k)
	}
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
