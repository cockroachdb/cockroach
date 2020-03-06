// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"crypto/tls"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

const (
	// NodeUser is used by nodes for intra-cluster traffic.
	NodeUser = "node"
	// RootUser is the default cluster administrator.
	RootUser = "root"
)

var certNamePattern struct {
	syncutil.Mutex
	re *regexp.Regexp
}

// UserAuthHook authenticates a user based on their username and whether their
// connection originates from a client or another node in the cluster.
type UserAuthHook func(string, bool) error

// SetCertNamePattern specifies a regular expression that is used to extract
// the "user" component from a certificate's common name field instead of
// treating the full common name as the user. If not specified, the default
// behavior is the same as specifying `(.*)`. The regex must specify exactly
// one parenthesized subexpression that will be captured.
func SetCertNamePattern(pattern string) error {
	re, err := regexp.Compile("^" + pattern + "$")
	if err != nil {
		return err
	}
	if re.NumSubexp() != 1 {
		return errors.Errorf("%q must specify exactly one parenthesized subexpression: %d",
			pattern, re.NumSubexp())
	}
	certNamePattern.Lock()
	certNamePattern.re = re
	certNamePattern.Unlock()
	return nil
}

func transformCommonName(commonName string) string {
	// If a certNamePattern is specified, use it to extract the component that
	// will be returned. We fail open. If the substring doesn't match we return
	// the entire common name field.
	certNamePattern.Lock()
	re := certNamePattern.re
	certNamePattern.Unlock()
	if re != nil {
		match := re.FindStringSubmatch(commonName)
		if len(match) == 2 {
			commonName = match[1]
		}
	}
	return commonName
}

// GetCertificateUser extract the username from a client certificate.
func GetCertificateUser(tlsState *tls.ConnectionState) (string, error) {
	if tlsState == nil {
		return "", errors.Errorf("request is not using TLS")
	}
	if len(tlsState.PeerCertificates) == 0 {
		return "", errors.Errorf("no client certificates in request")
	}
	// The go server handshake code verifies the first certificate, using
	// any following certificates as intermediates. See:
	// https://github.com/golang/go/blob/go1.8.1/src/crypto/tls/handshake_server.go#L723:L742
	commonName := tlsState.PeerCertificates[0].Subject.CommonName
	return transformCommonName(commonName), nil
}

// UserAuthCertHook builds an authentication hook based on the security
// mode and client certificate.
func UserAuthCertHook(insecureMode bool, tlsState *tls.ConnectionState) (UserAuthHook, error) {
	var certUser string

	if !insecureMode {
		var err error
		certUser, err = GetCertificateUser(tlsState)
		if err != nil {
			return nil, err
		}
	}

	return func(requestedUser string, clientConnection bool) error {
		// TODO(marc): we may eventually need stricter user syntax rules.
		if len(requestedUser) == 0 {
			return errors.New("user is missing")
		}

		if !clientConnection && requestedUser != NodeUser {
			return errors.Errorf("user %s is not allowed", requestedUser)
		}

		// If running in insecure mode, we have nothing to verify it against.
		if insecureMode {
			return nil
		}

		// The client certificate user must match the requested user,
		// except if the certificate user is NodeUser, which is allowed to
		// act on behalf of all other users.
		if !(certUser == NodeUser || certUser == requestedUser) {
			return errors.Errorf("requested user is %s, but certificate is for %s", requestedUser, certUser)
		}

		return nil
	}, nil
}

// UserAuthPasswordHook builds an authentication hook based on the security
// mode, password, and its potentially matching hash.
func UserAuthPasswordHook(insecureMode bool, password string, hashedPassword []byte) UserAuthHook {
	return func(requestedUser string, clientConnection bool) error {
		if len(requestedUser) == 0 {
			return errors.New("user is missing")
		}

		if !clientConnection {
			return errors.New("password authentication is only available for client connections")
		}

		if insecureMode {
			return nil
		}

		// If the requested user has an empty password, disallow authentication.
		if len(password) == 0 || CompareHashAndPassword(hashedPassword, password) != nil {
			return errors.Errorf(ErrPasswordUserAuthFailed, requestedUser)
		}

		return nil
	}
}

// ErrPasswordUserAuthFailed is the error template for failed password auth
// of a user. It should be used when the password is incorrect or the user
// does not exist.
const ErrPasswordUserAuthFailed = "password authentication failed for user %s"
