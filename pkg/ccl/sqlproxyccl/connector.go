// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	pgproto3 "github.com/jackc/pgproto3/v2"
)

// sessionRevivalTokenStartupParam indicates the name of the parameter that
// will activate token-based authentication if present in the startup message.
const sessionRevivalTokenStartupParam = "crdb:session_revival_token_base64"

// errRetryConnectorSentinel exists to allow more robust retection of retry
// errors even if they are wrapped.
var errRetryConnectorSentinel = errors.New("retry connector error")

// MarkAsRetriableConnectorError marks the given error with
// errRetryConnectorSentinel, which will trigger the connector to retry if such
// error returns.
func MarkAsRetriableConnectorError(err error) error {
	return errors.Mark(err, errRetryConnectorSentinel)
}

// IsRetriableConnectorError checks whether a given error is retriable. This
// should be called on errors which are transient so that the connector can
// retry on such errors.
func IsRetriableConnectorError(err error) bool {
	return errors.Is(err, errRetryConnectorSentinel)
}

// connector is a per-session tenant-associated component that can be used to
// obtain a connection to the tenant cluster. This will also handle the
// authentication phase. All connections returned by the connector should
// already be ready to accept regular pgwire messages (e.g. SQL queries).
type connector struct {
	// StartupMsg represents the startup message associated with the client.
	// This will be used when establishing a pgwire connection with the SQL pod.
	//
	// NOTE: This field is required.
	StartupMsg *pgproto3.StartupMessage

	// TLSConfig represents the client TLS config used by the connector when
	// connecting with the SQL pod. If the ServerName field is set, this will
	// be overridden during connection establishment. Set to nil if we are
	// connecting to an insecure cluster.
	//
	// NOTE: This field is optional.
	TLSConfig *tls.Config

	// AddrLookupFn is used by the connector to return an address (that must
	// include both host and port) pointing to one of the SQL pods for the
	// tenant associated with this connector.
	//
	// This will be called within an infinite backoff loop. If an error is
	// transient, this should return an error that has been marked with
	// errRetryConnectorSentinel (i.e. MarkAsRetriableConnectorError).
	//
	// NOTE: This field is required.
	AddrLookupFn func(ctx context.Context) (string, error)

	// AuthenticateFn is used by the connector to authenticate the client
	// against the server. This will only be used in non-token-based auth
	// methods. This should block until the server has authenticated the client.
	//
	// NOTE: This field is required.
	AuthenticateFn func(
		client net.Conn,
		server net.Conn,
		throttleHook func(throttler.AttemptStatus) error,
	) error

	// IdleMonitorWrapperFn is used to wrap the connection to the SQL pod with
	// an idle monitor. If not specified, the raw connection to the SQL pod
	// will be returned.
	//
	// In the case of connecting with an authentication phase, the connection
	// will be wrapped before starting the authentication.
	//
	// NOTE: This field is optional.
	IdleMonitorWrapperFn func(crdbConn net.Conn) net.Conn

	// Event callback functions. OnLookupEvent and OnDialEvent will be called
	// after the lookup and dial operations respectively, regardless of error.
	//
	// NOTE: These fields are optional.
	//
	// TODO(jaylim-crl): Look into removing event callback functions. This
	// requires us to pass in some sort of directory into the connector
	// component. Perhaps addrLookupFn could be replaced with that. We can't
	// do that today because addrLookupFn also relies on a fallback mechanism
	// for --routing-rule.
	OnLookupEvent func(ctx context.Context, err error)
	OnDialEvent   func(ctx context.Context, outgoingAddr string, err error)

	// Testing knobs for internal connector calls. If specified, these will
	// be called instead of the actual logic.
	testingKnobs struct {
		openClusterConnInternal func(ctx context.Context) (net.Conn, error)
		dialOutgoingAddr        func(outgoingAddr string) (net.Conn, error)
	}
}

// OpenClusterConnWithToken opens a connection to the tenant cluster using the
// token-based authentication.
func (c *connector) OpenClusterConnWithToken(ctx context.Context, token string) (net.Conn, error) {
	c.StartupMsg.Parameters[sessionRevivalTokenStartupParam] = token
	defer func() {
		// Delete token after return.
		delete(c.StartupMsg.Parameters, sessionRevivalTokenStartupParam)
	}()

	crdbConn, err := c.openClusterConnInternal(ctx)
	if err != nil {
		return nil, err
	}
	if c.IdleMonitorWrapperFn != nil {
		crdbConn = c.IdleMonitorWrapperFn(crdbConn)
	}
	return crdbConn, nil
}

// OpenClusterConnWithAuth opens a connection to the tenant cluster using
// normal authentication methods (e.g. password, etc.). Once a connection to
// one of the tenant's SQL pod has been established, we will transfer
// request/response flow between clientConn and the new connection to the
// authenticator, which implies that this will be blocked until authentication
// succeeds, or when an error is returned.
//
// sentToClient will be set to true if an error occurred during the
// authenticator phase since errors would have already been sent to the client.
func (c *connector) OpenClusterConnWithAuth(
	ctx context.Context, clientConn net.Conn, throttleHook func(throttler.AttemptStatus) error,
) (serverConn net.Conn, sentToClient bool, retErr error) {
	// Just a safety check, but this shouldn't happen since we will block the
	// startup param in the frontend admitter. The only case where we actually
	// need to delete this param is if OpenClusterConnWithToken was called
	// previously, but that wouldn't happen based on the current proxy logic.
	delete(c.StartupMsg.Parameters, sessionRevivalTokenStartupParam)

	crdbConn, err := c.openClusterConnInternal(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if retErr != nil {
			crdbConn.Close()
		}
	}()

	if c.IdleMonitorWrapperFn != nil {
		crdbConn = c.IdleMonitorWrapperFn(crdbConn)
	}

	// Perform user authentication.
	if err := c.AuthenticateFn(clientConn, crdbConn, throttleHook); err != nil {
		return nil, true, err
	}
	return crdbConn, false, nil
}

// openClusterConnInternal returns a connection to the tenant cluster associated
// with the connector. Once a connection has been established, the pgwire
// startup message will be relayed to the server. Returned errors may be marked
// as a lookup or dial error.
func (c *connector) openClusterConnInternal(ctx context.Context) (net.Conn, error) {
	if c.testingKnobs.openClusterConnInternal != nil {
		return c.testingKnobs.openClusterConnInternal(ctx)
	}

	// Repeatedly try to make a connection until context is canceled, or until
	// we get a non-retriable error. This is preferable to terminating client
	// connections, because in most cases those connections will simply be
	// retried, further increasing load on the system.
	retryOpts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
	}

	var crdbConn net.Conn
	var outgoingAddr string
	var err error
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		// Retrieve a SQL pod address to connect to.
		outgoingAddr, err = c.AddrLookupFn(ctx)
		if c.OnLookupEvent != nil {
			c.OnLookupEvent(ctx, err)
		}
		if err != nil {
			if IsRetriableConnectorError(err) {
				continue
			}
			return nil, err
		}
		// Make a connection to the SQL pod.
		crdbConn, err = c.dialOutgoingAddr(outgoingAddr)
		if c.OnDialEvent != nil {
			c.OnDialEvent(ctx, outgoingAddr, err)
		}
		if err != nil {
			if IsRetriableConnectorError(err) {
				continue
			}
			return nil, err
		}
		return crdbConn, nil
	}

	// Since the retry loop above retries infinitely, the only possibility
	// where we will exit the loop is when context is cancelled.
	if errors.Is(err, context.Canceled) {
		return nil, err
	}
	// Loop exited at boundary, so mark previous error with cancellation.
	if ctxErr := ctx.Err(); err != nil && ctxErr != nil {
		return nil, errors.Mark(err, ctxErr)
	}
	panic("unreachable")
}

// dialOutgoingAddr dials the given outgoing address for the SQL pod, and
// forwards the startup message to it. If the connector specifies a TLS
// connection, it will also attempt to upgrade the PG connection to use TLS.
func (c *connector) dialOutgoingAddr(outgoingAddr string) (net.Conn, error) {
	if c.testingKnobs.dialOutgoingAddr != nil {
		return c.testingKnobs.dialOutgoingAddr(outgoingAddr)
	}

	// Use a TLS config if one was provided. If TLSConfig is nil, Clone will
	// return nil.
	tlsConf := c.TLSConfig.Clone()
	if tlsConf != nil {
		// outgoingAddr will always have a port. We use an empty string as the
		// default port as we only care about extracting the host.
		outgoingHost, _, err := addr.SplitHostPort(outgoingAddr, "" /* defaultPort */)
		if err != nil {
			return nil, err
		}
		// Always set ServerName. If InsecureSkipVerify is true, this will
		// be ignored.
		tlsConf.ServerName = outgoingHost
	}

	conn, err := BackendDial(c.StartupMsg, outgoingAddr, tlsConf)
	if err != nil {
		var codeErr *codeError
		if errors.As(err, &codeErr) && codeErr.code == codeBackendDown {
			return nil, MarkAsRetriableConnectorError(err)
		}
		return nil, err
	}
	return conn, nil
}
