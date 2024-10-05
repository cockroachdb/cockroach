// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cluster

import (
	"crypto/tls"
	"net/http"
	"time"
)

// HTTPClient is an http.Client configured for querying a cluster. We need to
// run with "InsecureSkipVerify" (at least on Docker) due to the fact that we
// cannot use a fixed hostname to reach the cluster. This in turn means that we
// do not have a verified server name in the certs.
var HTTPClient = http.Client{
	Timeout: 3 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	},
}
