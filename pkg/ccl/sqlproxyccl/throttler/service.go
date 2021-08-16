// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package throttler provides admission checks functionality. Rate limiting currently.
package throttler

// ConnectionTags contains the parameters the throttle service uses to determine
// if a connection attempt should be allowed.
type ConnectionTags struct {
	// IP address of the client.
	IP string
	// ID of the tenant database the client is connecting to.
	TenantID string
}

// Service provides the interface for performing throttle checks before
// requests into the managed service system.
type Service interface {
	// LoginCheck determines whether a login request should be allowed to
	// proceed. It rate limits login attempts from IP addresses.
	LoginCheck(connection ConnectionTags) error

	// Report a successful login to the throttle.
	ReportSuccess(connection ConnectionTags)
}
