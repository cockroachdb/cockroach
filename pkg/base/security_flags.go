// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import (
	"strings"

	"github.com/cockroachdb/errors"
)

// SecurityOverrides stores the boolean flags that override security protections
// inside CockroachDB.
type SecurityOverrides int

const (
	// DisableTLS disables all the TLS configuration.
	DisableTLS SecurityOverrides = 1 << iota
	// DisableRPCTLS disables usage of TLS for RPC connections. This impacts
	// all types of RPC connections: node-node, tenant-node and client-node.
	DisableRPCTLS
	// DisableRPCAuthn disables RPC authentication.
	// Currently implied by DisableRPCTLS, because the only authentication
	// mechanism supported for RPC conns is TLS. We may be able to separate
	// them in the future by implementing a non-TLS RPC authn mechanism.
	// See also: https://github.com/cockroachdb/cockroach/issues/54007
	DisableRPCAuthn
	// DisableSQLAuthn disables authentication of SQL clients, i.e.
	// accept all connections as if processed via the 'trust' authentication
	// method.
	DisableSQLAuthn
	// DisableSQLTLS disables the availability of TLS connections for SQL clients.
	// Implied by DisableTLS.
	// When this option is not specified, SQL clients may, but do not have to,
	// use a TLS connection (ie. non-TLS connections are still allowed).
	DisableSQLTLS
	// DisableSQLRequireTLS disables the requirement that SQL clients use a TLS
	// connection.
	// When this option is not specified, SQL clients must use a TLS connection.
	DisableSQLRequireTLS
	// DisableSQLSetCredentials prevents SQL clients from setting new
	// password credentials on roles/users via the CREATE/ALTER USER/ROLE WITH PASSWORD
	// statement.
	// (They can still remove credentials though, or set password
	// credentials if they have write access to system.users)
	DisableSQLSetCredentials
	// DisableHTTPTLS disables TLS for HTTP connections.
	// Implied by DisableTLS.
	DisableHTTPTLS
	// DisableHTTPAuthn disables user authentication for HTTP requests.
	DisableHTTPAuthn
	// DisableRemoteCertsRetrieval disables the retrieval of TLS certificates
	// using the 'Certificates' RPC / API.
	// Implied by DisableRPCTLS, DisableRPCAuthn, DisableHTTPTLS or DisableHTTPAuthn.
	DisableRemoteCertsRetrieval
	// DisableClusterNameVerification alters the cluster name
	// verification to only verify that a non-empty cluster name on
	// both sides match. This is meant for use while rolling an
	// existing cluster into using a new cluster name.
	DisableClusterNameVerification

	// lastSecurityOverride must be last in the list above.
	lastSecurityOverride
	// DisableAll includes all the flags above.
	DisableAll SecurityOverrides = lastSecurityOverride - 1
)

var flagNames = map[SecurityOverrides]string{
	DisableTLS:                     "disable-tls",
	DisableRPCTLS:                  "disable-rpc-tls",
	DisableRPCAuthn:                "disable-rpc-authn",
	DisableSQLAuthn:                "disable-sql-authn",
	DisableSQLTLS:                  "disable-sql-tls",
	DisableSQLRequireTLS:           "disable-sql-require-tls",
	DisableSQLSetCredentials:       "disable-sql-set-credentials",
	DisableHTTPTLS:                 "disable-http-tls",
	DisableHTTPAuthn:               "disable-http-authn",
	DisableRemoteCertsRetrieval:    "disable-remote-certs-retrieval",
	DisableClusterNameVerification: "disable-cluster-name-verification",
}

var flagFromName = func() map[string]SecurityOverrides {
	m := make(map[string]SecurityOverrides)
	for k, v := range flagNames {
		k.Validate()
		m[v] = k
	}
	// DisableAll is special: it can be set from string, but
	// it does not have a distinct name.
	m["disable-all"] = DisableAll
	return m
}()

// String implements the fmt.Stringer interface.
func (o SecurityOverrides) String() string {
	var buf strings.Builder
	for i := SecurityOverrides(1); i < DisableAll; i = i * 2 {
		if o&i != 0 {
			if buf.Len() > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(flagNames[i])
		}
	}
	return buf.String()
}

// Type implements the pflag.Value interface.
func (o SecurityOverrides) Type() string { return "<override flags>" }

// Set implements the pflag.Value interface.
func (o *SecurityOverrides) Set(v string) error {
	parts := strings.Split(v, ",")
	for _, part := range parts {
		part = strings.ToLower(strings.TrimSpace(part))
		flag, ok := flagFromName[part]
		if !ok {
			return errors.Newf("unknown security override: %q", part)
		}
		o.addInternal(flag)
	}
	o.Validate()
	return nil
}

// IsSet checks whether the given flags are all set.
func (o SecurityOverrides) IsSet(flag SecurityOverrides) bool {
	return o&flag == flag
}

// AddOverride adds an override.
func (o *SecurityOverrides) AddOverride(flag SecurityOverrides) {
	o.addInternal(flag)
	o.Validate()
}

func (o *SecurityOverrides) addInternal(flag SecurityOverrides) {
	*o = *o | flag
}

// Validate checks the combination of flags and propagates
// flag implications.
func (o *SecurityOverrides) Validate() {
	if o.IsSet(DisableTLS) {
		o.addInternal(DisableRPCTLS | DisableSQLTLS | DisableHTTPTLS)
	}
	if o.IsSet(DisableRPCTLS) || o.IsSet(DisableRPCAuthn) {
		o.addInternal(DisableRPCTLS | DisableRPCAuthn)
	}
	if o.IsSet(DisableSQLTLS) {
		o.addInternal(DisableSQLRequireTLS)
	}
	if o.IsSet(DisableRPCTLS) || o.IsSet(DisableRPCAuthn) || o.IsSet(DisableHTTPTLS) || o.IsSet(DisableHTTPAuthn) {
		o.addInternal(DisableRemoteCertsRetrieval)
	}
}
