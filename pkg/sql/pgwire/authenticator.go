// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/go-ldap/ldap/v3"
)

// Authenticator is a component of an AuthMethod that determines if the
// given system identity (e.g.: Kerberos or X.509 principal, plain-old
// username, etc) is who it claims to be.
type Authenticator = func(
	ctx context.Context,
	systemIdentity string,
	clientConnection bool,
	pwRetrieveFn PasswordRetrievalFn,
	roleSubject *ldap.DN,
) error

// PasswordRetrievalFn defines a method to retrieve a hashed password
// and expiration time for a user logging in with password-based
// authentication.
type PasswordRetrievalFn = func(context.Context) (
	expired bool,
	pwHash password.PasswordHash,
	_ error,
)
