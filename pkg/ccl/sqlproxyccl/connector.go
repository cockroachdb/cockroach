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
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	pgproto3 "github.com/jackc/pgproto3/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sessionRevivalTokenStartupParam indicates the name of the parameter that
// will activate token-based authentication if present in the startup message.
const sessionRevivalTokenStartupParam = "crdb:session_revival_token_base64"

// remoteAddrStartupParam contains the remote address of the original client.
const remoteAddrStartupParam = "crdb:remote_addr"

// TenantResolver is an interface for the tenant directory. Currently only
// tenant.Directory implements it.
//
// TODO(jaylim-crl): Rename this to Directory, and the current tenant.Directory
// to tenant.directory. This needs to be moved into the tenant package as well.
// This is added here to aid testing.
type TenantResolver interface {
	// EnsureTenantAddr returns an IP address of one of the given tenant's SQL
	// processes based on the tenantID and clusterName fields. This should block
	// until the process associated with the IP is ready.
	//
	// If no matching pods are found (e.g. cluster name mismatch, or tenant was
	// deleted), this will return a GRPC NotFound error.
	EnsureTenantAddr(
		ctx context.Context,
		tenantID roachpb.TenantID,
		clusterName string,
	) (string, error)

	// LookupTenantAddrs returns the IP addresses for all available SQL
	// processes for the given tenant. It returns a GRPC NotFound error if the
	// tenant does not exist.
	//
	// Unlike EnsureTenantAddr which blocks until there is an associated
	// process, LookupTenantAddrs will just return an empty set if no processes
	// are available for the tenant.
	LookupTenantAddrs(ctx context.Context, tenantID roachpb.TenantID) ([]string, error)

	// ReportFailure is used to indicate to the resolver that a connection
	// attempt to connect to a particular SQL tenant pod have failed.
	ReportFailure(ctx context.Context, tenantID roachpb.TenantID, addr string) error
}

// connector is a per-session tenant-associated component that can be used to
// obtain a connection to the tenant cluster. This will also handle the
// authentication phase. All connections returned by the connector should
// already be ready to accept regular pgwire messages (e.g. SQL queries).
type connector struct {
	// ClusterName and TenantID corresponds to the tenant identifiers associated
	// with this connector.
	//
	// NOTE: These fields are required.
	ClusterName string
	TenantID    roachpb.TenantID

	// Directory corresponds to the tenant directory, which will be used to
	// resolve tenants to their corresponding IP addresses. If this isn't set,
	// we will fallback to use RoutingRule.
	//
	// TODO(jaylim-crl): Replace this with a Directory interface, and remove
	// the RoutingRule field. RoutingRule should not be in here.
	//
	// NOTE: This field is optional.
	Directory TenantResolver

	// RoutingRule refers to the static rule that will be used when resolving
	// tenants. This will be used directly whenever the Directory field isn't
	// specified, or as a fallback if one was specified.
	//
	// The literal "{{clusterName}}" will be replaced with ClusterName within
	// the RoutingRule string.
	//
	// NOTE: This field is optional, if Directory isn't set.
	RoutingRule string

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

	// IdleMonitorWrapperFn is used to wrap the connection to the SQL pod with
	// an idle monitor. If not specified, the raw connection to the SQL pod
	// will be returned.
	//
	// In the case of connecting with an authentication phase, the connection
	// will be wrapped before starting the authentication.
	//
	// NOTE: This field is optional.
	IdleMonitorWrapperFn func(serverConn net.Conn) net.Conn

	// Testing knobs for internal connector calls. If specified, these will
	// be called instead of the actual logic.
	testingKnobs struct {
		dialTenantCluster func(ctx context.Context) (net.Conn, error)
		lookupAddr        func(ctx context.Context) (string, error)
		dialSQLServer     func(serverAddr string) (net.Conn, error)
	}
}

// OpenTenantConnWithToken opens a connection to the tenant cluster using the
// token-based authentication during connection migration.
func (c *connector) OpenTenantConnWithToken(
	ctx context.Context, token string,
) (retServerConn net.Conn, retErr error) {
	c.StartupMsg.Parameters[sessionRevivalTokenStartupParam] = token
	defer func() {
		// Delete token after return.
		delete(c.StartupMsg.Parameters, sessionRevivalTokenStartupParam)
	}()

	serverConn, err := c.dialTenantCluster(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			serverConn.Close()
		}
	}()

	if c.IdleMonitorWrapperFn != nil {
		serverConn = c.IdleMonitorWrapperFn(serverConn)
	}

	// When we use token-based authentication, we will still get the initial
	// connection data messages (e.g. ParameterStatus and BackendKeyData).
	// Since this method is only used during connection migration (i.e. proxy
	// is connecting to the SQL pod), we'll discard all of the messages, and
	// only return once we've seen a ReadyForQuery message.
	//
	// NOTE: This will need to be updated when we implement query cancellation.
	if err := readTokenAuthResult(serverConn); err != nil {
		return nil, err
	}
	log.Infof(ctx, "connected to %s through token-based auth", serverConn.RemoteAddr())
	return serverConn, nil
}

// OpenTenantConnWithAuth opens a connection to the tenant cluster using
// normal authentication methods (e.g. password, etc.). Once a connection to
// one of the tenant's SQL pod has been established, we will transfer
// request/response flow between clientConn and the new connection to the
// authenticator, which implies that this will be blocked until authentication
// succeeds, or when an error is returned.
//
// sentToClient will be set to true if an error occurred during the
// authenticator phase since errors would have already been sent to the client.
func (c *connector) OpenTenantConnWithAuth(
	ctx context.Context, clientConn net.Conn, throttleHook func(throttler.AttemptStatus) error,
) (retServerConn net.Conn, sentToClient bool, retErr error) {
	// Just a safety check, but this shouldn't happen since we will block the
	// startup param in the frontend admitter. The only case where we actually
	// need to delete this param is if OpenTenantConnWithToken was called
	// previously, but that wouldn't happen based on the current proxy logic.
	delete(c.StartupMsg.Parameters, sessionRevivalTokenStartupParam)

	serverConn, err := c.dialTenantCluster(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if retErr != nil {
			serverConn.Close()
		}
	}()

	if c.IdleMonitorWrapperFn != nil {
		serverConn = c.IdleMonitorWrapperFn(serverConn)
	}

	// Perform user authentication for non-token-based auth methods. This will
	// block until the server has authenticated the client.
	if err := authenticate(clientConn, serverConn, throttleHook); err != nil {
		return nil, true, err
	}
	log.Infof(ctx, "connected to %s through normal auth", serverConn.RemoteAddr())
	return serverConn, false, nil
}

// dialTenantCluster returns a connection to the tenant cluster associated
// with the connector. Once a connection has been established, the pgwire
// startup message will be relayed to the server.
func (c *connector) dialTenantCluster(ctx context.Context) (net.Conn, error) {
	if c.testingKnobs.dialTenantCluster != nil {
		return c.testingKnobs.dialTenantCluster(ctx)
	}

	// Repeatedly try to make a connection until context is canceled, or until
	// we get a non-retriable error. This is preferable to terminating client
	// connections, because in most cases those connections will simply be
	// retried, further increasing load on the system.
	retryOpts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
	}

	lookupAddrErr := log.Every(time.Minute)
	dialSQLServerErr := log.Every(time.Minute)
	reportFailureErr := log.Every(time.Minute)
	var lookupAddrErrs, dialSQLServerErrs, reportFailureErrs int
	var crdbConn net.Conn
	var serverAddr string
	var err error

	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		// Retrieve a SQL pod address to connect to.
		serverAddr, err = c.lookupAddr(ctx)
		if err != nil {
			if isRetriableConnectorError(err) {
				lookupAddrErrs++
				if lookupAddrErr.ShouldLog() {
					log.Ops.Errorf(ctx, "lookup address (%d errors skipped): %v",
						lookupAddrErrs, err)
					lookupAddrErrs = 0
				}
				continue
			}
			return nil, err
		}
		// Make a connection to the SQL pod.
		crdbConn, err = c.dialSQLServer(serverAddr)
		if err != nil {
			if isRetriableConnectorError(err) {
				dialSQLServerErrs++
				if dialSQLServerErr.ShouldLog() {
					log.Ops.Errorf(ctx, "dial SQL server (%d errors skipped): %v",
						dialSQLServerErrs, err)
					dialSQLServerErrs = 0
				}

				// Report the failure to the directory so that it can refresh
				// any stale information that may have caused the problem.
				if c.Directory != nil {
					if err = reportFailureToDirectory(
						ctx, c.TenantID, serverAddr, c.Directory,
					); err != nil {
						reportFailureErrs++
						if reportFailureErr.ShouldLog() {
							log.Ops.Errorf(ctx,
								"report failure (%d errors skipped): %v",
								reportFailureErrs,
								err,
							)
							reportFailureErrs = 0
						}
					}
				}
				continue
			}
			return nil, err
		}
		return crdbConn, nil
	}

	// err will never be nil here regardless of whether we retry infinitely or
	// a bounded number of times. In our case, since we retry infinitely, the
	// only possibility is when ctx's Done channel is closed (which implies that
	// ctx.Err() != nil.
	//
	// If the error is already marked, just return that.
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return nil, err
	}
	// Otherwise, mark the error, and return that.
	return nil, errors.Mark(err, ctx.Err())
}

// resolveTCPAddr indirection to allow test hooks.
var resolveTCPAddr = net.ResolveTCPAddr

// lookupAddr returns an address (that must include both host and port)
// pointing to one of the SQL pods for the tenant associated with this
// connector.
//
// This will be called within an infinite backoff loop. If an error is
// transient, this will return an error that has been marked with
// errRetryConnectorSentinel (i.e. markAsRetriableConnectorError).
func (c *connector) lookupAddr(ctx context.Context) (string, error) {
	if c.testingKnobs.lookupAddr != nil {
		return c.testingKnobs.lookupAddr(ctx)
	}

	// First try to lookup tenant in the directory (if available).
	if c.Directory != nil {
		addr, err := c.Directory.EnsureTenantAddr(ctx, c.TenantID, c.ClusterName)
		if err != nil {
			if status.Code(err) != codes.NotFound {
				return "", markAsRetriableConnectorError(err)
			}
			// Fallback to old resolution rule.
		} else {
			return addr, nil
		}
	}

	// Derive DNS address and then try to resolve it. If it does not exist, then
	// map to a GRPC NotFound error.
	//
	// TODO(jaylim-crl): This code is temporary. Remove this once we have fully
	// replaced this with a Directory interface. This fallback does not need
	// to exist.
	addr := strings.ReplaceAll(
		c.RoutingRule, "{{clusterName}}",
		fmt.Sprintf("%s-%d", c.ClusterName, c.TenantID.ToUint64()),
	)
	if _, err := resolveTCPAddr("tcp", addr); err != nil {
		log.Errorf(ctx, "could not retrieve SQL server address: %v", err.Error())
		return "", newErrorf(codeParamsRoutingFailed,
			"cluster %s-%d not found", c.ClusterName, c.TenantID.ToUint64())
	}
	return addr, nil
}

// dialSQLServer dials the given address for the SQL pod, and forwards the
// startup message to it. If the connector specifies a TLS connection, it will
// also attempt to upgrade the PG connection to use TLS.
//
// This will be called within an infinite backoff loop. If an error is
// transient, this will return an error that has been marked with
// errRetryConnectorSentinel (i.e. markAsRetriableConnectorError).
func (c *connector) dialSQLServer(serverAddr string) (net.Conn, error) {
	if c.testingKnobs.dialSQLServer != nil {
		return c.testingKnobs.dialSQLServer(serverAddr)
	}

	// Use a TLS config if one was provided. If TLSConfig is nil, Clone will
	// return nil.
	tlsConf := c.TLSConfig.Clone()
	if tlsConf != nil {
		// serverAddr will always have a port. We use an empty string as the
		// default port as we only care about extracting the host.
		outgoingHost, _, err := addr.SplitHostPort(serverAddr, "" /* defaultPort */)
		if err != nil {
			return nil, err
		}
		// Always set ServerName. If InsecureSkipVerify is true, this will
		// be ignored.
		tlsConf.ServerName = outgoingHost
	}

	conn, err := BackendDial(c.StartupMsg, serverAddr, tlsConf)
	if err != nil {
		var codeErr *codeError
		if errors.As(err, &codeErr) && codeErr.code == codeBackendDown {
			return nil, markAsRetriableConnectorError(err)
		}
		return nil, err
	}
	return conn, nil
}

// errRetryConnectorSentinel exists to allow more robust retection of retry
// errors even if they are wrapped.
var errRetryConnectorSentinel = errors.New("retry connector error")

// markAsRetriableConnectorError marks the given error with
// errRetryConnectorSentinel, which will trigger the connector to retry if such
// error returns.
func markAsRetriableConnectorError(err error) error {
	return errors.Mark(err, errRetryConnectorSentinel)
}

// isRetriableConnectorError checks whether a given error is retriable. This
// should be called on errors which are transient so that the connector can
// retry on such errors.
func isRetriableConnectorError(err error) bool {
	return errors.Is(err, errRetryConnectorSentinel)
}

// reportFailureToDirectory is a hookable function that calls the given tenant
// directory's ReportFailure method.
var reportFailureToDirectory = func(
	ctx context.Context, tenantID roachpb.TenantID, addr string, directory TenantResolver,
) error {
	return directory.ReportFailure(ctx, tenantID, addr)
}
