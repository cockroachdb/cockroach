// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/password"
)

// Authenticator is a component of an AuthMethod that determines if the
// given system identity (e.g.: Kerberos or X.509 principal, plain-old
// username, etc) is who it claims to be.
type Authenticator = func(
	ctx context.Context,
	systemIdentity security.SQLUsername,
	clientConnection bool,
	pwRetrieveFn PasswordRetrievalFn,
) error

// PasswordRetrievalFn defines a method to retrieve a hashed password
// and expiration time for a user logging in with password-based
// authentication.
type PasswordRetrievalFn = func(context.Context) (
	expired bool,
	pwHash password.PasswordHash,
	_ error,
)
