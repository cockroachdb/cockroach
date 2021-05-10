// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// Timeout is a cluster setting used for cloud storage interactions.
var Timeout = settings.RegisterDurationSetting(
	"cloudstorage.timeout",
	"the timeout for import/export storage operations",
	10*time.Minute,
).WithPublic()

var httpCustomCA = settings.RegisterStringSetting(
	"cloudstorage.http.custom_ca",
	"custom root CA (appended to system's default CAs) for verifying certificates when interacting with HTTPS storage",
	"",
).WithPublic()

// HTTPRetryOptions defines the tunable settings which control the retry of HTTP
// operations.
var HTTPRetryOptions = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     2 * time.Second,
	MaxRetries:     32,
	Multiplier:     4,
}

func MakeHTTPClient(settings *cluster.Settings) (*http.Client, error) {
	var tlsConf *tls.Config
	if pem := httpCustomCA.Get(&settings.SV); pem != "" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.Wrap(err, "could not load system root CA pool")
		}
		if !roots.AppendCertsFromPEM([]byte(pem)) {
			return nil, errors.Errorf("failed to parse root CA certificate from %q", pem)
		}
		tlsConf = &tls.Config{RootCAs: roots}
	}
	// Copy the defaults from http.DefaultTransport. We cannot just copy the
	// entire struct because it has a sync Mutex. This has the unfortunate problem
	// that if Go adds fields to DefaultTransport they won't be copied here,
	// but this is ok for now.
	t := http.DefaultTransport.(*http.Transport)
	return &http.Client{Transport: &http.Transport{
		Proxy:                 t.Proxy,
		DialContext:           t.DialContext,
		MaxIdleConns:          t.MaxIdleConns,
		IdleConnTimeout:       t.IdleConnTimeout,
		TLSHandshakeTimeout:   t.TLSHandshakeTimeout,
		ExpectContinueTimeout: t.ExpectContinueTimeout,

		// Add our custom CA.
		TLSClientConfig: tlsConf,
	}}, nil
}
