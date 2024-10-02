// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clientsecopts

import (
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
)

// ClientSecurityOptions defines the configurable connection parameters
// used as input to compute the security parameters of connection URLs.
type ClientSecurityOptions struct {
	// Insecure corresponds to the --insecure flag.
	Insecure bool

	// CertsDir corresponds to the --certs-dir flag.
	CertsDir string
}

// loadSecurityOptions extends a url.Values with SSL settings suitable for
// the given server config.
func loadSecurityOptions(
	opts ClientSecurityOptions, u *pgurl.URL, user username.SQLUsername,
) error {
	u.WithUsername(user.Normalized())
	if opts.Insecure {
		u.WithInsecure()
	} else if net, _, _ := u.GetNetworking(); net == pgurl.ProtoTCP {
		tlsUsed, tlsMode, caCertPath := u.GetTLSOptions()
		if !tlsUsed {
			// TLS explicitly disabled by client. Nothing to do here.
			return nil
		}
		// Default is to verify the server's identity.
		if tlsMode == pgurl.TLSUnspecified {
			tlsMode = pgurl.TLSVerifyFull
		}

		loader := securityassets.GetLoader()
		cl := certnames.MakeLocator(opts.CertsDir)

		// Only verify-full and verify-ca should be doing certificate verification.
		if tlsMode == pgurl.TLSVerifyFull || tlsMode == pgurl.TLSVerifyCA {
			if caCertPath == "" {
				// We need a CA certificate.
				// Try to use the cert locator to find one, and if that fails,
				// assume that the Go TLS code will fall back to a OS-level
				// common trust store.

				exists, err := loader.FileExists(cl.CACertPath())
				if err != nil {
					return err
				}
				if exists {
					// The CL has found a CA cert. Use that.
					caCertPath = cl.CACertPath()
				}
			}
			// Fallback: if caCertPath was not assigned above, either
			// we did not have a certs dir, or it did not contain
			// a CA cert. In that case, we rely on the OS trust store.
			// Documentation of tls.Config:
			//     https://pkg.go.dev/crypto/tls#Config
		}

		// (Re)populate the transport information.
		u.WithTransport(pgurl.TransportTLS(tlsMode, caCertPath))

		// Fetch client certs, but don't fail if they're absent, we may be
		// using a password.
		certPath := cl.ClientCertPath(user)
		keyPath := cl.ClientKeyPath(user)
		certsAvailable := checkCertAndKeyAvailable(certPath, keyPath)

		// If the command specifies user node, and we did not find
		// client.node.crt, try with just node.crt.
		if !certsAvailable && user.IsNodeUser() {
			certPath = cl.NodeCertPath()
			keyPath = cl.NodeKeyPath()
			certsAvailable = checkCertAndKeyAvailable(certPath, keyPath)
		}

		// If we found some certs, add them to the URL authentication
		// method.
		if certsAvailable {
			pwEnabled, hasPw, pwd := u.GetAuthnPassword()
			if !pwEnabled {
				u.WithAuthn(pgurl.AuthnClientCert(certPath, keyPath))
			} else {
				u.WithAuthn(pgurl.AuthnPasswordAndCert(certPath, keyPath, hasPw, pwd))
			}
		}
	}
	return nil
}

// ServerParameters is used to pass a copy of the parameters computed
// when a SQL server starts up, to the input of PGURL(), to create
// a connection URL that clients can connect to.
type ServerParameters struct {
	// ServerAddr is the configured server address to use if the URL does not contain one.
	ServerAddr string

	// DefaultPort is the port number to use if ServerAddr does not contain one.
	DefaultPort string

	// DefaultDatabase is the default database name to use if the connection URL
	// does not contain one.
	DefaultDatabase string
}

// MakeURLForServer constructs a URL for the postgres endpoint, given a server
// config.
func MakeURLForServer(
	copts ClientSecurityOptions, sparams ServerParameters, user *url.Userinfo,
) (*pgurl.URL, error) {
	host, port, _ := addr.SplitHostPort(sparams.ServerAddr, sparams.DefaultPort)
	u := pgurl.New().
		WithNet(pgurl.NetTCP(host, port)).
		WithDatabase(sparams.DefaultDatabase)

	username, _ := username.MakeSQLUsernameFromUserInput(user.Username(), username.PurposeValidation)
	if err := loadSecurityOptions(copts, u, username); err != nil {
		return nil, err
	}
	return u, nil
}

// ClientOptions represents configurable options from the command line.
type ClientOptions struct {
	ClientSecurityOptions

	// ExplicitURL is an explicit URL specified on the command line
	// via --url, and enhanced via clientconnurl.UpdateURL().
	// This may contain additional parameters (e.g. application_name)
	// which cannot be specified by discrete flags.
	ExplicitURL *pgurl.URL

	// User is the requested username, as specified via --user or --url.
	//
	// When calling UpdateURL() to generate ExplicitURL from --url, the
	// function should be passed an update callback that updates this
	// field if the URL contains a username part.
	User string

	// Database is the requested database name, as specified
	// via --database or --url.
	//
	// When calling UpdateURL() to generate ExplicitURL from --url, the
	// function should be passed an update callback that updates this
	// field if the URL contains a database name.
	Database string

	// ServerHost is the requested server host name or address, as
	// specified via --host or --url.
	//
	// When calling UpdateURL() to generate ExplicitURL from --url, the
	// function should be passed an update callback that updates this
	// field if the URL contains a hostname part.
	ServerHost string

	// ServerPort is the requested server port name/number, as
	// specified via --port or --url.
	//
	// When calling UpdateURL() to generate ExplicitURL from --url, the
	// function should be passed an update callback that updates this
	// field if the URL contains a port number/name.
	ServerPort string
}

// MakeClientConnURL constructs a connection URL from the given input options.
func MakeClientConnURL(copts ClientOptions) (*pgurl.URL, error) {
	purl := copts.ExplicitURL
	if purl == nil {
		// New URL. Start from scratch.
		purl = pgurl.New() // defaults filled in below.
	}

	// Fill in any defaults from any command-line arguments if there was
	// no --url flag, or if they were specified *after* the --url flag.
	//
	// Note: the username is filled in by loadSecurityOptions() below.
	// If there was any password while parsing a --url flag,
	// it will be pre-populated via cliCtx.sqlConnURL above.
	purl.WithDatabase(copts.Database)
	if _, host, port := purl.GetNetworking(); host != copts.ServerHost || port != copts.ServerPort {
		purl.WithNet(pgurl.NetTCP(copts.ServerHost, copts.ServerPort))
	}

	// Check the structure of the username.
	userName, err := username.MakeSQLUsernameFromUserInput(copts.User, username.PurposeValidation)
	if err != nil {
		return nil, err
	}
	if userName.Undefined() {
		userName = username.RootUserName()
	}

	if err := loadSecurityOptions(copts.ClientSecurityOptions, purl, userName); err != nil {
		return nil, err
	}

	// The construct above should have produced a valid URL already;
	// however a post-assertion doesn't hurt.
	if err := purl.Validate(); err != nil {
		return nil, err
	}

	return purl, nil
}

func checkCertAndKeyAvailable(certPath string, keyPath string) bool {
	loader := securityassets.GetLoader()
	_, err1 := loader.Stat(certPath)
	_, err2 := loader.Stat(keyPath)
	return err1 == nil && err2 == nil
}
