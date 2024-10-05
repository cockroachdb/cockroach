// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package certmgr

import (
	"context"
	"crypto/tls"
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Ensure that FileCert implements Cert.
var _ Cert = (*FileCert)(nil)

// FileCert represents a single cert loaded from cert/key file pair.
type FileCert struct {
	syncutil.Mutex
	certFile string
	keyFile  string
	err      error
	cert     *tls.Certificate
}

// NewFileCert will construct a new file cert but it will not try to load it.
// A follow up Reload is needed to read, parse and verify the actual cert/key files.
func NewFileCert(certFile, keyFile string) *FileCert {
	return &FileCert{
		certFile: certFile,
		keyFile:  keyFile,
	}
}

// Reload the certificate from files.
func (fc *FileCert) Reload(ctx context.Context) {
	fc.Lock()
	defer fc.Unlock()

	// There was a previous error that is not yet retrieved.
	if fc.err != nil {
		return
	}

	certBytes, err := os.ReadFile(fc.certFile)
	if err != nil {
		fc.err = errors.Wrapf(err, "could not reload cert file %s", fc.certFile)
		return
	}

	keyBytes, err := os.ReadFile(fc.keyFile)
	if err != nil {
		fc.err = errors.Wrapf(err, "could not reload cert key file %s", fc.keyFile)
		return
	}

	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		fc.err = errors.Wrapf(err, "could not construct cert from cert %s and key %s", fc.certFile, fc.keyFile)
		return
	}

	fc.cert = &cert
}

// Err will return the last error that occurred during reload or nil if the
// last reload was successful.
func (fc *FileCert) Err() error {
	fc.Lock()
	defer fc.Unlock()
	return fc.err
}

// ClearErr will clear the last err so the follow up Reload can execute.
func (fc *FileCert) ClearErr() {
	fc.Lock()
	defer fc.Unlock()
	fc.err = nil
}

// TLSCert returns the tls certificate if the load was successful.
func (fc *FileCert) TLSCert() *tls.Certificate {
	return fc.cert
}
