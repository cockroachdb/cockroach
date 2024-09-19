// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
	"github.com/go-ldap/ldap/v3"
)

// AuthBehaviors encapsulates the per-connection behaviors that may be
// configured. This type is returned by AuthMethod implementations.
//
// Callers should call the AuthBehaviors.ConnClose method once the
// associated network connection has been terminated to allow external
// resources to be released.
type AuthBehaviors struct {
	authenticator       Authenticator
	connClose           func()
	replacementIdentity string
	replacedIdentity    bool
	roleMapper          RoleMapper
	authorizer          Authorizer
}

// Ensure that an AuthBehaviors is easily composable with itself.
var _ Authenticator = (*AuthBehaviors)(nil).Authenticate
var _ func() = (*AuthBehaviors)(nil).ConnClose
var _ RoleMapper = (*AuthBehaviors)(nil).MapRole
var _ Authorizer = (*AuthBehaviors)(nil).MaybeAuthorize

// This is a hack for the unused-symbols linter. These two functions
// are, at present, only called by the GSSAPI integration. The code
// is guarded by a build tag, which is ignored by the linter.
// TODO(#dev-inf): Update the linter to include the "gss" build tag.
var _ = (*AuthBehaviors)(nil).SetConnClose
var _ = (*AuthBehaviors)(nil).SetReplacementIdentity

// Authenticate delegates to the Authenticator passed to SetAuthenticator or
// returns an error if SetAuthenticator has not been called.
func (b *AuthBehaviors) Authenticate(
	ctx context.Context,
	systemIdentity string,
	clientConnection bool,
	pwRetrieveFn PasswordRetrievalFn,
	roleSubject *ldap.DN,
) error {
	if found := b.authenticator; found != nil {
		return found(ctx, systemIdentity, clientConnection, pwRetrieveFn, roleSubject)
	}
	return errors.New("no Authenticator provided to AuthBehaviors")
}

// SetAuthenticator updates the Authenticator to be used.
func (b *AuthBehaviors) SetAuthenticator(a Authenticator) {
	b.authenticator = a
}

// ConnClose delegates to the function passed to SetConnClose to release
// any resources associated with the connection. This method is a no-op
// if SetConnClose has not been called or was called with nil.
func (b *AuthBehaviors) ConnClose() {
	if fn := b.connClose; fn != nil {
		fn()
	}
}

// SetConnClose updates the connection-close callback.
func (b *AuthBehaviors) SetConnClose(fn func()) {
	b.connClose = fn
}

// ReplacementIdentity returns an optional replacement for the
// client-provided identity when validating the incoming connection.
// This allows "ambient" authentication mechanisms, such as GSSAPI, to
// provide replacement values. This method will return ok==false if
// SetReplacementIdentity has not been called.
func (b *AuthBehaviors) ReplacementIdentity() (_ string, ok bool) {
	return b.replacementIdentity, b.replacedIdentity
}

// SetReplacementIdentity allows the AuthMethod to override the
// client-reported system identity.
func (b *AuthBehaviors) SetReplacementIdentity(id string) {
	b.replacementIdentity = id
	b.replacedIdentity = true
}

// MapRole delegates to the RoleMapper passed to SetRoleMapper or
// returns an error if SetRoleMapper has not been called.
func (b *AuthBehaviors) MapRole(
	ctx context.Context, systemIdentity string,
) ([]username.SQLUsername, error) {
	if found := b.roleMapper; found != nil {
		return found(ctx, systemIdentity)
	}
	return nil, errors.New("no RoleMapper provided to AuthBehaviors")
}

// SetRoleMapper updates the RoleMapper to be used.
func (b *AuthBehaviors) SetRoleMapper(m RoleMapper) {
	b.roleMapper = m
}

// SetAuthorizer updates the SetAuthorizer to be used.
func (b *AuthBehaviors) SetAuthorizer(a Authorizer) {
	b.authorizer = a
}

// MaybeAuthorize delegates to the Authorizer passed to SetAuthorizer and if
// successful obtains the grants using RoleGranter passed to SetRoleGranter.
func (b *AuthBehaviors) MaybeAuthorize(
	ctx context.Context, systemIdentity string, clientConnection bool,
) error {
	if found := b.authorizer; found != nil {
		return found(ctx, systemIdentity, clientConnection)
	}
	return nil
}
