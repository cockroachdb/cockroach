// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package cli

import (
	"context"
	"crypto/x509"
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// reConnRefused is a regular expression that can be applied
// to the details of a GRPC connection failure.
//
// On *nix, a connect error looks like:
//    dial tcp <addr>: <syscall>: connection refused
// On Windows, it looks like:
//    dial tcp <addr>: <syscall>: No connection could be made because the target machine actively refused it.
// So we look for the common bit.
var reGRPCConnRefused = regexp.MustCompile(`Error while dialing dial tcp .*: connection.* refused`)

// reGRPCNoTLS is a regular expression that can be applied to the
// details of a GRPC auth failure when the server is insecure.
var reGRPCNoTLS = regexp.MustCompile(`authentication handshake failed: tls: first record does not look like a TLS handshake`)

// reGRPCAuthFailure is a regular expression that can be applied to
// the details of a GRPC auth failure when the SSL handshake fails.
var reGRPCAuthFailure = regexp.MustCompile(`authentication handshake failed: x509`)

// reGRPCConnFailed is a regular expression that can be applied
// to the details of a GRPC connection failure when, perhaps,
// the server was expecting a TLS handshake but the client didn't
// provide one (i.e. the client was started with --insecure).
// Note however in that case it's not certain what the problem is,
// as the same error could be raised for other reasons.
var reGRPCConnFailed = regexp.MustCompile(`desc = (transport is closing|all SubConns are in TransientFailure)`)

// MaybeDecorateGRPCError catches grpc errors and provides a more helpful error
// message to the user.
func MaybeDecorateGRPCError(
	wrapped func(*cobra.Command, []string) error,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := wrapped(cmd, args)

		if err == nil {
			return nil
		}

		extraInsecureHint := func() string {
			extra := ""
			if baseCfg.Insecure {
				extra = "\nIf the node is configured to require secure connections,\n" +
					"remove --insecure and configure secure credentials instead.\n"
			}
			return extra
		}

		connFailed := func() error {
			const format = "cannot dial server.\n" +
				"Is the server running?\n" +
				"If the server is running, check --host client-side and --advertise server-side.\n\n%v"
			return errors.Errorf(format, err)
		}

		connSecurityHint := func() error {
			const format = "SSL authentication error while connecting.\n%s\n%v"
			return errors.Errorf(format, extraInsecureHint(), err)
		}

		connInsecureHint := func() error {
			return errors.Errorf("cannot establish secure connection to insecure server.\n"+
				"Maybe use --insecure?\n\n%v", err)
		}

		connRefused := func() error {
			extra := extraInsecureHint()
			return errors.Errorf("server closed the connection.\n"+
				"Is this a CockroachDB node?\n"+extra+"\n%v", err)
		}

		// Is this an "unable to connect" type of error?
		unwrappedErr := errors.Cause(err)

		if unwrappedErr == pq.ErrSSLNotSupported {
			// SQL command failed after establishing a TCP connection
			// successfully, but discovering that it cannot use TLS while it
			// expected the server supports TLS.
			return connInsecureHint()
		}

		switch wErr := unwrappedErr.(type) {
		case *security.Error:
			return errors.Errorf("cannot load certificates.\n"+
				"Check your certificate settings, set --certs-dir, or use --insecure for insecure clusters.\n\n%v",
				unwrappedErr)

		case *x509.UnknownAuthorityError:
			// A SQL connection was attempted with an incorrect CA.
			return connSecurityHint()

		case *initialSQLConnectionError:
			// SQL handshake failed after establishing a TCP connection
			// successfully, something else than CockroachDB responded, was
			// confused and closed the door on us.
			return connRefused()

		case *pq.Error:
			// SQL commands will fail with a pq error but only after
			// establishing a TCP connection successfully. So if we got
			// here, there was a TCP connection already.

			// Did we fail due to security settings?
			if wErr.Code == pgcode.ProtocolViolation {
				return connSecurityHint()
			}
			// Otherwise, there was a regular SQL error. Just report that.
			return wErr

		case *net.OpError:
			// A non-RPC client command was used (e.g. a SQL command) and an
			// error occurred early while establishing the TCP connection.

			// Is this a TLS error?
			if msg := wErr.Err.Error(); strings.HasPrefix(msg, "tls: ") {
				// Error during the SSL handshake: a provided client
				// certificate was invalid, but the CA was OK. (If the CA was
				// not OK, we'd get a x509 error, see case above.)
				return connSecurityHint()
			}
			return connFailed()

		case *netutil.InitialHeartbeatFailedError:
			// A GRPC TCP connection was established but there was an early failure.
			// Try to distinguish the cases.
			msg := wErr.Error()
			if reGRPCConnRefused.MatchString(msg) {
				return connFailed()
			}
			if reGRPCNoTLS.MatchString(msg) {
				return connInsecureHint()
			}
			if reGRPCAuthFailure.MatchString(msg) {
				return connSecurityHint()
			}
			if reGRPCConnFailed.MatchString(msg) {
				return connRefused()
			}

			// Other cases may be timeouts or other situations, these
			// will be handled below.
		}

		opTimeout := func() error {
			return errors.Errorf("operation timed out.\n\n%v", err)
		}

		// Is it a plain context cancellation (i.e. timeout)?
		switch unwrappedErr {
		case context.DeadlineExceeded:
			return opTimeout()
		case context.Canceled:
			return opTimeout()
		}

		// Is it a GRPC-observed context cancellation (i.e. timeout), a GRPC
		// connection error, or a known indication of a too-old server?
		if code := status.Code(unwrappedErr); code == codes.DeadlineExceeded {
			return opTimeout()
		} else if code == codes.Unimplemented &&
			strings.Contains(unwrappedErr.Error(), "unknown method Decommission") ||
			strings.Contains(unwrappedErr.Error(), "unknown service cockroach.server.serverpb.Init") {
			return fmt.Errorf(
				"incompatible client and server versions (likely server version: v1.0, required: >=v1.1)")
		} else if grpcutil.IsClosedConnection(unwrappedErr) {
			return errors.Errorf("connection lost.\n\n%v", err)
		}

		// Does the server require GSSAPI authentication?
		if strings.Contains(unwrappedErr.Error(), "pq: unknown authentication response: 7") {
			return fmt.Errorf(
				"server requires GSSAPI authentication for this user.\n" +
					"The CockroachDB CLI does not support GSSAPI authentication; use 'psql' instead")
		}

		// Nothing we can special case, just return what we have.
		return err
	}
}

// maybeShoutError calls log.Shout on errors, better surfacing problems to the user.
func maybeShoutError(
	wrapped func(*cobra.Command, []string) error,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := wrapped(cmd, args)
		return checkAndMaybeShout(err)
	}
}

func checkAndMaybeShout(err error) error {
	if err == nil {
		return nil
	}
	severity := log.Severity_ERROR
	cause := err
	if ec, ok := errors.Cause(err).(*cliError); ok {
		severity = ec.severity
		cause = ec.cause
	}
	log.Shout(context.Background(), severity, cause)
	return err
}
