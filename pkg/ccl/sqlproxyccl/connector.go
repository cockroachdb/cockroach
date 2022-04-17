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

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/balancer"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
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

	// DirectoryCache corresponds to the tenant directory cache, which will be
	// used to resolve tenants to their corresponding IP addresses.
	//
	// NOTE: This field is required.
	DirectoryCache tenant.DirectoryCache

	// Balancer represents the load balancer component that will be used to
	// choose which SQL pod to route the connection to.
	//
	// NOTE: This field is required.
	Balancer *balancer.Balancer

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
		dialTenantCluster func(ctx context.Context, opts *dialOptions) (net.Conn, error)
		lookupValidAddr   func(ctx context.Context, dstAddr string) (string, error)
		dialSQLServer     func(serverAddr string) (net.Conn, error)
	}
}

// OpenTenantConnWithToken opens a connection to the tenant cluster at the pod
// with the given address using the token-based authentication. This is only
// used during connection migration.
//
// NOTE: dstAddr, if not empty, has to be associated with the connector's tenant,
// and has to point to a RUNNING pod.
func (c *connector) OpenTenantConnWithToken(
	ctx context.Context, token string, dstAddr string,
) (retServerConn net.Conn, retErr error) {
	c.StartupMsg.Parameters[sessionRevivalTokenStartupParam] = token
	defer func() {
		// Delete token after return.
		delete(c.StartupMsg.Parameters, sessionRevivalTokenStartupParam)
	}()

	serverConn, err := c.dialTenantCluster(ctx, &dialOptions{dstAddr: dstAddr})
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

	serverConn, err := c.dialTenantCluster(ctx, &dialOptions{})
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

// dialOptions controls the behavior dialTenantCluster.
type dialOptions struct {
	// dstAddr represents the address to dial. This has to be a RUNNING pod for
	// the given tenant, or the dial will fail. If this is empty, a pod will be
	// chosen based on the pod's selection algorithm.
	dstAddr string
}

// dialTenantCluster returns a connection to the tenant cluster associated
// with the connector. Once a connection has been established, the pgwire
// startup message will be relayed to the server.
func (c *connector) dialTenantCluster(ctx context.Context, opts *dialOptions) (net.Conn, error) {
	if c.testingKnobs.dialTenantCluster != nil {
		return c.testingKnobs.dialTenantCluster(ctx, opts)
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

	// TODO(jaylim-crl): Update dialOptions to take in a configurable retry
	// policy. It is unnecessary to retry infinitely in the case of a transfer.
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		// Retrieve a SQL pod address to connect to.
		//
		// NOTE: Even if dstAddr was provided, it is important to validate that
		// the pod is RUNNING, and belongs to the given tenant.
		serverAddr, err = c.lookupValidAddr(ctx, opts.dstAddr)
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

				// Report the failure to the directory cache so that it can
				// refresh any stale information that may have caused the
				// problem.
				if err = reportFailureToDirectoryCache(
					ctx, c.TenantID, serverAddr, c.DirectoryCache,
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

// lookupValidAddr returns an address (that includes both host and port)
// pointing to one of the SQL pods for the tenant associated with this
// connector. If dstAddr is not empty, that address will be validated, and if
// valid, will be returned instead; if dstAddr is not valid, a non-retriable
// error will be returned.
//
// This will be called within an infinite backoff loop. If an error is transient,
// this will return an error that has been marked with errRetryConnectorSentinel
// (i.e. markAsRetriableConnectorError).
func (c *connector) lookupValidAddr(ctx context.Context, dstAddr string) (string, error) {
	if c.testingKnobs.lookupValidAddr != nil {
		return c.testingKnobs.lookupValidAddr(ctx, dstAddr)
	}

	// Lookup tenant in the directory cache.
	pods, err := c.DirectoryCache.LookupTenantPods(ctx, c.TenantID, c.ClusterName)
	if err != nil {
		switch status.Code(err) {
		case codes.FailedPrecondition:
			if st, ok := status.FromError(err); ok {
				return "", newErrorf(codeUnavailable, "%v", st.Message())
			}
			return "", newErrorf(codeUnavailable, "unavailable")
		case codes.NotFound:
			return "", newErrorf(codeParamsRoutingFailed,
				"cluster %s-%d not found", c.ClusterName, c.TenantID.ToUint64())
		default:
			return "", markAsRetriableConnectorError(err)
		}
	}

	// Filter for RUNNING pods, and validate dstAddr if supplied.
	validDstAddr := false
	runningPods := make([]*tenant.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod.State != tenant.RUNNING {
			continue
		}

		// If the RUNNING pod matches dstAddr, it is considered valid.
		if pod.Addr == dstAddr {
			validDstAddr = true
		}

		runningPods = append(runningPods, pod)
	}

	// A destination address was supplied.
	if dstAddr != "" {
		if !validDstAddr {
			return "", errors.Newf("could not connect to invalid address '%s'", dstAddr)
		}
		return dstAddr, nil
	}

	// Use the pod selection algorithm to choose a pod's address.
	pod, err := c.Balancer.SelectTenantPod(runningPods)
	if err != nil {
		// This should never happen because LookupTenantPods ensured that
		// there should be at least one RUNNING pod. Mark it as a retriable
		// connection anyway.
		return "", markAsRetriableConnectorError(err)
	}
	return pod.Addr, nil
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

// reportFailureToDirectoryCache is a hookable function that calls the given
// tenant directory cache's ReportFailure method.
var reportFailureToDirectoryCache = func(
	ctx context.Context,
	tenantID roachpb.TenantID,
	addr string,
	directoryCache tenant.DirectoryCache,
) error {
	return directoryCache.ReportFailure(ctx, tenantID, addr)
}
