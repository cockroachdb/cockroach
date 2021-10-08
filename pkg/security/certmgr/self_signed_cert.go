// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package certmgr

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Ensure that SelfSignedCert implements Cert.
var _ Cert = (*SelfSignedCert)(nil)

// SelfSignedCert represents a single, self-signed certificate, generated at runtime.
type SelfSignedCert struct {
	syncutil.Mutex
	years, months, days int
	secs                time.Duration
	err                 error
	cert                *tls.Certificate
}

// NewSelfSignedCert will generate a new self-signed cert.
// A follow up Reload will regenerate the cert.
func NewSelfSignedCert(years, months, days int, secs time.Duration) *SelfSignedCert {
	return &SelfSignedCert{years: years, months: months, days: days, secs: secs}
}

// Reload will regenerate the self-signed cert.
func (ssc *SelfSignedCert) Reload(ctx context.Context) {
	ssc.Lock()
	defer ssc.Unlock()

	// There was a previous error that is not yet retrieved.
	if ssc.err != nil {
		return
	}

	// Generate self signed cert for testing.
	// Use "openssl s_client -showcerts -starttls postgres -connect {HOSTNAME}:{PORT}" to
	// inspect the certificate or save the certificate portion to a file and
	// use sslmode=verify-full
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		ssc.err = errors.Wrapf(err, "could not generate key")
		return
	}

	from := timeutil.Now()
	until := from.AddDate(ssc.years, ssc.months, ssc.days).Add(ssc.secs)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    from,
		NotAfter:     until,
		DNSNames:     []string{"localhost"},
		IsCA:         true,
	}
	cer, err := x509.CreateCertificate(rand.Reader, &template, &template, pub, priv)
	if err != nil {
		ssc.err = errors.Wrapf(err, "could not create certificate")
	}

	ssc.cert = &tls.Certificate{
		Certificate: [][]byte{cer},
		PrivateKey:  priv,
	}
}

// Err will return the last error that occurred during reload or nil if the
// last reload was successful.
func (ssc *SelfSignedCert) Err() error {
	ssc.Lock()
	defer ssc.Unlock()
	return ssc.err
}

// ClearErr will clear the last err so the follow up Reload can execute.
func (ssc *SelfSignedCert) ClearErr() {
	ssc.Lock()
	defer ssc.Unlock()
	ssc.err = nil
}

// TLSCert returns the tls certificate if the cert generation was successful.
func (ssc *SelfSignedCert) TLSCert() *tls.Certificate {
	return ssc.cert
}
