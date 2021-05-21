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

import "net/url"

// Scheme is the URL scheme used in this package.
// We have observed that all drivers support "postgresql",
// whereas some drivers do not support "postgres". So we use
// the longer form here for maximum compatibility.
const Scheme = "postgresql"

// URL represents a family of postgres connection strings pointing
// to the same server.
type URL struct {
	username string
	database string

	// Which network protocol to use.
	net NetProtocol
	// Which transport to use.
	sec transportType
	// Which authentication mechanism to use.
	authn authnType

	// Network parameters.
	// With unix sockets, host is the socket directory.
	host, port string

	// Transport parameters.
	caCertPath string

	// Authentication parameters.
	clientCertPath string
	clientKeyPath  string

	hasPassword bool
	password    string

	// Ancillary options.
	extraOptions url.Values
}

// New creates a new URL.
func New() *URL {
	return &URL{
		net:   ProtoTCP,
		sec:   tnUnspecified,
		authn: authnNone,
	}
}

// WithUsername configures which username to use for the client session.
func (u *URL) WithUsername(s string) *URL {
	u.username = s
	return u
}

// WithDefaultUsername inserts a username if not already present in the URL.
func (u *URL) WithDefaultUsername(user string) *URL {
	if u.username == "" {
		u.username = user
	}
	return u
}

// GetUsername retrieves the username inside the URL.
func (u *URL) GetUsername() string {
	return u.username
}

// WithDatabase configures which database to use in the initial connection.
func (u *URL) WithDatabase(s string) *URL {
	u.database = s
	return u
}

// WithDefaultDatabase inserts a username if not already present in the URL.
func (u *URL) WithDefaultDatabase(db string) *URL {
	if u.database == "" {
		u.database = db
	}
	return u
}

// GetDatabase retrieves the database inside the URL.
func (u *URL) GetDatabase() string {
	return u.database
}

// AddOptions adds key=value options to the URL.
// Certain combinations are checked and an error is returned
// if a combination is found invalid.
func (u *URL) AddOptions(opts url.Values) error {
	return u.parseOptions(opts)
}

// SetOption sets one option in the URL.
// This also ensures that there is only one value for the
// given option.
// Certain combinations are checked and an error is returned
// if a combination is found invalid.
func (u *URL) SetOption(key, value string) error {
	vals := []string{value}
	opts := url.Values{key: vals}
	if err := u.parseOptions(opts); err != nil {
		return err
	}
	if _, ok := u.extraOptions[key]; ok {
		u.extraOptions[key] = vals
	}
	return nil
}

// GetOption retrieves the last value for the given extra option.
func (u *URL) GetOption(opt string) string {
	return getVal(u.extraOptions, opt)
}

// WithInsecure configures the URL for CockroachDB servers running with
// all security controls disabled.
func (u *URL) WithInsecure() *URL {
	return u.
		WithUsername("root").
		WithAuthn(AuthnNone()).
		WithTransport(TransportNone())
}

// NetProtocol is the type of networking used in a URL.
type NetProtocol int

const (
	// ProtoUndefined is used when the type of networking is not known yet.
	ProtoUndefined NetProtocol = iota
	// ProtoUnix indicates the URL uses a unix datagram socket.
	ProtoUnix
	// ProtoTCP indicates the URL uses TCP/IP.
	ProtoTCP
)

// NetOption is the type of the valid arguments to WithNet.
type NetOption func(u *URL) *URL

// WithNet configures which network protocol to use for the URL.
func (u *URL) WithNet(opt NetOption) *URL {
	return opt(u)
}

// NetTCP creates an option to use the specified host and port over TCP.
func NetTCP(host, port string) NetOption {
	return NetOption(func(u *URL) *URL {
		u.net = ProtoTCP
		u.host = host
		u.port = port
		return u
	})
}

// NetUnix creates an option to use a unix datagram socket in the
// specified directory.
func NetUnix(socketDir, port string) NetOption {
	return NetOption(func(u *URL) *URL {
		u.net = ProtoUnix
		u.host = socketDir
		u.port = port
		return u
	})
}

// WithDefaultHost inserts a host value if not already present in the URL.
func (u *URL) WithDefaultHost(host string) *URL {
	if u.host == "" {
		u.host = host
	}
	return u
}

// WithDefaultPort inserts a port value if not already present in the URL.
func (u *URL) WithDefaultPort(port string) *URL {
	if u.port == "" {
		u.port = port
	}
	return u
}

// GetNetworking retrieves the network protocol and address details
// from the URL. For Unix sockets, the 1st returned string value
// is the path to the socket directory; for TCP connections,
// it is the hostname / server address.
func (u *URL) GetNetworking() (NetProtocol, string, string) {
	return u.net, u.host, u.port
}

type authnType int

const (
	authnUndefined authnType = iota
	authnNone
	authnPassword
	authnClientCert
	authnPasswordWithClientCert
)

// AuthnOption is the type of the valid arguments to WithAuthn.
type AuthnOption func(u *URL) *URL

// WithAuthn configures which authentication method to use in the URL.
func (u *URL) WithAuthn(opt AuthnOption) *URL {
	return opt(u)
}

// AuthnNone creates an option to not use any client authentication.
func AuthnNone() AuthnOption {
	return AuthnOption(func(u *URL) *URL {
		u.authn = authnNone
		return u
	})
}

// AuthnPassword creates an option to use password authentication.
// If setPassword is false, the URL will be populated in a way that
// makes the client prompt for a password.
func AuthnPassword(setPassword bool, password string) AuthnOption {
	return AuthnOption(func(u *URL) *URL {
		// FIXME: balk at gen time if password is empty
		u.authn = authnPassword
		u.hasPassword = setPassword
		u.password = password
		return u
	})
}

// GetAuthnPassword returns whether password authentication is in use,
// and if yes, whether the password is defined and the possible password.
func (u *URL) GetAuthnPassword() (authnPwdEnabled bool, hasPassword bool, password string) {
	return u.authn == authnPassword || u.authn == authnPasswordWithClientCert,
		u.hasPassword,
		u.password
}

// AuthnClientCert creates an option to use TLS client cert authn.
func AuthnClientCert(clientCertPath, clientKeyPath string) AuthnOption {
	return AuthnOption(func(u *URL) *URL {
		u.authn = authnClientCert
		u.clientCertPath = clientCertPath
		u.clientKeyPath = clientKeyPath
		return u
	})
}

// GetAuthnCert returns whether client cert authentication is in use,
// and if yes, the paths to the client cert details.
func (u *URL) GetAuthnCert() (authnCertEnabled bool, clientCertPath, clientKeyPath string) {
	return u.authn == authnClientCert || u.authn == authnPasswordWithClientCert,
		u.clientCertPath,
		u.clientKeyPath
}

// AuthnPasswordAndCert creates an option to use both password and TLS
// client cert authn.
// (At the time of this writing, this mode is not supported by CockroachDB
// but it is supported by postgres and may be supported by crdb later.)
func AuthnPasswordAndCert(
	clientCertPath, clientKeyPath string, setPassword bool, password string,
) AuthnOption {
	p := AuthnPassword(setPassword, password)
	c := AuthnClientCert(clientCertPath, clientKeyPath)
	return AuthnOption(func(u *URL) *URL {
		u = c(p(u))
		u.authn = authnPasswordWithClientCert
		return u
	})
}

type transportType string

const (
	tnUnspecified   transportType = ""
	tnNone          transportType = "disable"
	tnTLSVerifyFull transportType = "verify-full"
	tnTLSVerifyCA   transportType = "verify-ca"
	tnTLSRequire    transportType = "require"
	tnTLSPrefer     transportType = "prefer"
	tnTLSAllow      transportType = "allow"
)

// TLSMode is the type of arguments to TransportTLS options.
type TLSMode transportType

const (
	// TLSVerifyFull checks the server's name against its certificate.
	TLSVerifyFull TLSMode = TLSMode(tnTLSVerifyFull)
	// TLSVerifyCA only checks the server cert is signed by the known root CA.
	TLSVerifyCA TLSMode = TLSMode(tnTLSVerifyCA)
	// TLSRequire requires TLS but does not validate the server sert.
	// It allows MITM attacks.
	TLSRequire TLSMode = TLSMode(tnTLSRequire)
	// TLSPrefer uses TLS if available, but does not require it.
	TLSPrefer TLSMode = TLSMode(tnTLSPrefer)
	// TLSAllow uses TLS if the server requires it.
	TLSAllow TLSMode = TLSMode(tnTLSAllow)
	// TLSUnspecified lets the client driver decide the TLS options.
	TLSUnspecified TLSMode = TLSMode(tnUnspecified)
)

// TransportOption is the type of the valid arguments to WithTransport.
type TransportOption func(u *URL) *URL

// WithTransport configures which transport protocol to use in the URL.
func (u *URL) WithTransport(opt TransportOption) *URL {
	return opt(u)
}

// TransportTLS creates an option to use a TLS transport.
// The second argument is optional and specified the path to the CA certificate.
func TransportTLS(mode TLSMode, caCertPath string) TransportOption {
	return TransportOption(func(u *URL) *URL {
		u.sec = transportType(mode)
		u.caCertPath = caCertPath
		return u
	})
}

// TransportNone creates an option to use a pass-through transport.
// This disables TLS.
func TransportNone() TransportOption {
	return TransportOption(func(u *URL) *URL {
		u.sec = tnNone
		return u
	})
}

// GetTLSOptions retrieves the transport options, and if TLS is used,
// the path to the CA certificate if specified inside the URL.
func (u *URL) GetTLSOptions() (tlsUsed bool, mode TLSMode, caCertPath string) {
	if u.sec == tnNone {
		return false, "", ""
	}
	return true, TLSMode(u.sec), u.caCertPath
}
