// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgurl

import (
	"net"
	"net/url"
	"sort"
	"strings"
)

// ToDSN converts the URL to a connection DSN, suitable
// for drivers such as psycopg.
func (u *URL) ToDSN() string {
	escaper := strings.NewReplacer(` `, `\ `, `'`, `\'`, `\`, `\\`)
	var s strings.Builder

	accrue := func(key, val string) {
		s.WriteByte(' ')
		s.WriteString(key)
		s.WriteByte('=')
		s.WriteString(escaper.Replace(val))
	}
	if u.database != "" {
		accrue("database", u.database)
	}
	if u.username != "" {
		accrue("user", u.username)
	}
	switch u.net {
	case ProtoUnix, ProtoTCP:
		if u.host != "" {
			accrue("host", u.host)
		}
		if u.port != "" {
			accrue("port", u.port)
		}
	}
	switch u.authn {
	case authnPassword, authnPasswordWithClientCert:
		if u.hasPassword {
			accrue("password", u.password)
		}
	}
	switch u.authn {
	case authnClientCert, authnPasswordWithClientCert:
		accrue("sslcert", u.clientCertPath)
		accrue("sslkey", u.clientKeyPath)
	}

	if u.caCertPath != "" {
		accrue("sslrootcert", u.caCertPath)
	}

	if u.sec != tnUnspecified {
		accrue("sslmode", string(u.sec))
	}

	keys := make([]string, len(u.extraOptions))
	for k := range u.extraOptions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := u.extraOptions[k]
		for _, val := range v {
			accrue(k, val)
		}
	}

	out := s.String()
	if len(out) > 0 {
		// Remove the heading space on the way out.
		out = out[1:]
	}
	return out
}

// ToPQ converts the URL to a connection string supported
// by drivers using libpq or compatible.
func (u *URL) ToPQ() *url.URL {
	nu, opts := u.baseURL()

	if u.username != "" {
		nu.User = url.User(u.username)
	}
	switch u.authn {
	case authnPassword, authnPasswordWithClientCert:
		if u.hasPassword {
			nu.User = url.UserPassword(u.username, u.password)
		}
	}

	nu.RawQuery = opts.Encode()
	return nu
}

// String makes URL printable.
func (u *URL) String() string { return u.ToPQ().String() }

// ToJDBC converts the URL to a connection string supported
// by drivers using jdbc or compatible.
func (u *URL) ToJDBC() *url.URL {
	nu, opts := u.baseURL()

	nu.Scheme = "jdbc:" + nu.Scheme

	if u.username != "" {
		opts.Set("user", u.username)
	}
	switch u.authn {
	case authnPassword, authnPasswordWithClientCert:
		if u.hasPassword {
			opts.Set("password", u.password)
		}
	}

	nu.RawQuery = opts.Encode()
	return nu
}

// baseURL constructs the URL fields common to both
// PQ-style and JDBC-style URLs.
func (u *URL) baseURL() (*url.URL, url.Values) {
	nu := &url.URL{
		Scheme: Scheme,
		Path:   "/" + u.database,
	}
	opts := url.Values{}
	for k, v := range u.extraOptions {
		opts[k] = v
	}

	switch u.net {
	case ProtoTCP:
		nu.Host = u.host
		if u.port != "" {
			nu.Host = net.JoinHostPort(u.host, u.port)
		}
	case ProtoUnix:
		// Ensure u.Host is not set: some client drivers ignore a query
		// arg 'host' if the hostname is set in the URL.
		opts.Set("host", u.host)
		if u.port != "" {
			opts.Set("port", u.port)
		}
	}

	switch u.authn {
	case authnClientCert, authnPasswordWithClientCert:
		opts.Set("sslcert", u.clientCertPath)
		opts.Set("sslkey", u.clientKeyPath)
	}

	if u.caCertPath != "" {
		opts.Set("sslrootcert", u.caCertPath)
	}

	if u.sec != tnUnspecified {
		opts.Set("sslmode", string(u.sec))
	}

	return nu, opts
}
