// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package throttler provides admission checks functionality. Rate limiting currently.
package throttler

import (
	"context"
	"time"
)

// ConnectionTags contains the parameters the throttle service uses to determine
// if a connection attempt should be allowed.
type ConnectionTags struct {
	// IP address of the client.
	IP string
	// ID of the tenant database the client is connecting to.
	TenantID string
}

// AttemptStatus is used to communicate to the throttler if the authentication attempt
// was successful or not.
type AttemptStatus int

const (
	// AttemptOK indicates the client's provided the correct user and password.
	AttemptOK AttemptStatus = iota

	// AttemptInvalidCredentials indicates something went wrong during authentication.
	AttemptInvalidCredentials
)

// Service provides the interface for performing throttle checks before
// requests into the managed service system.
type Service interface {
	// LoginCheck determines whether a login request should be allowed to
	// proceed. It rate limits login attempts from IP addresses.
	LoginCheck(ctx context.Context, connection ConnectionTags) (time.Time, error)

	// Report an authentication attempt. The throttleTime is used to
	// retroactively throttle the request if a racing request triggered the
	// throttle. The caller should pass the time returned by LoginCheck. If
	// ReportAttempt returns an error, the SQL proxy should return a throttle
	// error instead of authentication success/failure. This limits the
	// information a malicious user gets from using racing requests to guess
	// multiple passwords in one throttle window.
	ReportAttempt(ctx context.Context, connection ConnectionTags, throttleTime time.Time, status AttemptStatus) error
}
