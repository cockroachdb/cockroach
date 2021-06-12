// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package throttler provides admission checks functionality. Rate limiting currently.
package throttler

import "time"

// Service provides the interface for performing throttle checks before
// requests into the managed service system.
type Service interface {
	// LoginCheck determines whether a login request should be allowed to
	// proceed. It rate limits login attempts from IP addresses and it rate
	// limits login attempts to individual accounts.
	LoginCheck(ipAddress string, now time.Time) error
}
