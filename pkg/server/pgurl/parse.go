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
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/errors"
)

// Parse parses a URL connection string and extracts its properties.
// The resulting URL object contains the same data as the input URL,
// but that does not guarantee the URL is valid. To verify this,
// use the Validate() method.
func Parse(s string) (*URL, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	if u.Opaque != "" {
		return nil, errors.Newf("unknown URL format: %s", s)
	}

	dst := New()

	sc := strings.TrimPrefix(u.Scheme, "jdbc:")
	if sc != "postgres" && sc != "postgresql" {
		return nil, errors.Newf("unrecognized URL scheme: %s", u.Scheme)
	}

	if u.User != nil {
		dst.username = u.User.Username()
		dst.password, dst.hasPassword = u.User.Password()
	}

	if u.Host != "" {
		host, port, err := netutil.SplitHostPort(u.Host, "0")
		if err != nil {
			return nil, err
		}
		dst.host = host
		if port != "0" {
			dst.port = port
		}
	}

	if u.Path != "" {
		// Strip the leading / in the path.
		dst.database = u.Path[1:]
	}

	q := u.Query()

	err = dst.parseOptions(q)
	return dst, err
}

func (u *URL) parseOptions(extra url.Values) error {
	q := u.extraOptions
	if q == nil {
		// Copy the input keys. We don't want to modify the caller's map.
		q = make(url.Values)
		for k, v := range extra {
			q[k] = v
		}
	} else {
		// Add the new extra options to our map.
		for u, vs := range extra {
			q[u] = append(q[u], vs...)
		}
	}

	if _, hasUserOpt := q["user"]; hasUserOpt {
		u.username = getVal(q, "user")
		delete(q, "user")
	}
	if _, hasHostOpt := q["host"]; hasHostOpt {
		u.host = getVal(q, "host")
		delete(q, "host")
	}
	if _, hasPortOpt := q["port"]; hasPortOpt {
		u.port = getVal(q, "port")
		delete(q, "port")
	}
	_, hasPasswordOpt := q["password"]
	if hasPasswordOpt {
		u.hasPassword = true
		u.password = getVal(q, "password")
		delete(q, "password")
	}
	_, hasRootCert := q["sslrootcert"]
	if hasRootCert {
		u.caCertPath = getVal(q, "sslrootcert")
		delete(q, "sslrootcert")
	}
	_, hasClientCert := q["sslcert"]
	if hasClientCert {
		u.clientCertPath = getVal(q, "sslcert")
		delete(q, "sslcert")
	}
	_, hasClientKey := q["sslkey"]
	if hasClientKey {
		u.clientKeyPath = getVal(q, "sslkey")
		delete(q, "sslkey")
	}

	// Detect the network type.
	if strings.HasPrefix(u.host, "/") {
		u.net = ProtoUnix
	} else {
		u.net = ProtoTCP
	}

	// Detect the authentication type.
	useCerts := hasClientCert || hasClientKey || u.clientCertPath != "" || u.clientKeyPath != ""
	if u.hasPassword && !useCerts {
		u.authn = authnPassword
	} else if u.hasPassword && useCerts {
		u.authn = authnPasswordWithClientCert
	} else if !u.hasPassword && useCerts {
		u.authn = authnClientCert
	} else {
		u.authn = authnNone
	}

	// Detect the transport.
	if _, hasSSLMode := q["sslmode"]; hasSSLMode {
		sslMode := getVal(q, "sslmode")
		delete(q, "sslmode")
		switch sslMode {
		case string(tnNone),
			string(tnTLSVerifyFull),
			string(tnTLSVerifyCA),
			string(tnTLSRequire),
			string(tnTLSPrefer),
			string(tnTLSAllow):
			u.sec = transportType(sslMode)
		default:
			return errors.Newf("unrecognized sslmode parameter in URL: %s", sslMode)
		}
	}

	u.extraOptions = q

	return nil
}

// getVal is like Values.Get() but it retrieves the last instance.
func getVal(q url.Values, key string) string {
	v := q[key]
	if len(v) == 0 {
		return ""
	}
	return v[len(v)-1]
}
