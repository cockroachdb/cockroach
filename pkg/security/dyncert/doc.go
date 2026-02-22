// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package dyncert provides dynamic certificate handling with atomic rotation support.

This package enables certificate updates without restarting services or
reconnecting clients. It's designed for long-running systems that need to rotate
certificates (e.g., for expiration or security updates) without downtime.

The key types are Cert (an atomic certificate holder) and Manager (a file-based
certificate loader with SIGHUP reload support).

# Usage

Basic certificate usage with typed accessors:

	cert := dyncert.NewCert(certPEM, keyPEM)

	// For server/client authentication (requires private key):
	tlsCert, err := cert.AsTLSCertificate()
	if err != nil {
	    // handle error
	}

	// For CA verification (private key optional):
	pool, err := cert.AsCAPool()
	if err != nil {
	    // handle error
	}

Certificate rotation with Manager:

	// Create manager tied to stopper lifecycle.
	mgr, err := dyncert.NewManager(ctx, stopper)
	if err != nil {
	    // handle error
	}

	// Register certificates with file paths.
	err = mgr.Register("server", "/etc/certs/server.crt", "/etc/certs/server.key")
	if err != nil {
	    // handle error
	}
	err = mgr.Register("ca", "/etc/certs/ca.crt", "")
	if err != nil {
	    // handle error
	}

	// Use certificates in TLS config.
	serverCert := mgr.Get("server")
	caCert := mgr.Get("ca")
	tlsConfig := dyncert.NewTLSConfig(serverCert, caCert)

Dynamic TLS configuration for mTLS:

	// Create a TLS config that automatically detects certificate updates.
	config := dyncert.NewTLSConfig(
	    myServerCert,
	    caCertForVerifyingClients,
	    dyncert.WithPeerCN("expected-client-name"),
	)

	// Use for servers:
	listener, err := tls.Listen("tcp", ":8443", config)

	// Or for clients:
	conn, err := tls.Dial("tcp", "server:8443", config)

# Design (for maintainers)

Cert type design:
The Cert type stores PEM data and lazily caches parsed forms (tls.Certificate,
x509.CertPool). The cache is invalidated on Set() calls. Cached pointers are
stable until the next Set(), enabling change detection via pointer comparison.

This design makes rotation efficient - callers can hold references to Cert
objects and those references automatically see new certificate data after Set().
The atomic Set() operation ensures no partial updates are visible.

Manager reload mechanism:
The Manager registers signal handlers for SIGHUP and reloads all certificates
from disk. This is the standard Unix pattern for graceful config reloads.
NewManager registers the signal handler synchronously before spawning the
background goroutine, ensuring no signals are missed. The goroutine stops
automatically when the stopper quiesces.

Dynamic TLS implementation:
NewTLSConfig returns a tls.Config with callbacks that check for certificate
updates on each handshake. The callbacks (GetClientCertificate, GetConfigForClient,
VerifyConnection) call refresh methods that use pointer comparison to detect
changes. This means handshakes only rebuild TLS objects when certificates have
actually changed.

The InsecureSkipVerify + VerifyConnection pattern is required for client-side
dynamic CA rotation. InsecureSkipVerify disables Go's built-in verification so
we can use our own CA pool, and VerifyConnection replicates the full standard
verification logic (chain validation, hostname checking, key usage).

Server-side verification uses the standard ClientCAs + ClientAuth mechanism
because GetConfigForClient returns a fresh config for each connection.
*/
package dyncert
