// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"crypto/tls"
	"time"
)

// Admitter provides the interface for performing admission checks before
// allowing requests into sqlproxy.
type Admitter interface {
	// LoginCheck determines whether a request should be allowed to proceed.
	LoginCheck(ipAddress string, now time.Time) error
}

// BackendConfig contains the configuration of a backend connection that is
// being proxied.
type BackendConfig struct {
	ClientID string
	Address  string
	TLSConf  *tls.Config
	Admitter Admitter
}
