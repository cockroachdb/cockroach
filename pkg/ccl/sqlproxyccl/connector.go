// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
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

	// DialTenantLatency tracks how long it takes to retrieve the address for
	// a tenant and set up a tcp connection to the address.
	DialTenantLatency metric.IHistogram

	// DialTenantRetries counts how often dialing a tenant is retried.
	DialTenantRetries *metric.Counter

	// CancelInfo contains the data used to implement pgwire query cancellation.
	// It is only populated after authenticating the connection.
	CancelInfo *cancelInfo

	// Testing knobs for internal connector calls. If specified, these will
	// be called instead of the actual logic.
	testingKnobs struct {
		dialTenantCluster func(ctx context.Context, requester balancer.ConnectionHandle) (net.Conn, error)
		lookupAddr        func(ctx context.Context) (string, error)
		dialSQLServer     func(serverAssignment *balancer.ServerAssignment) (net.Conn, error)
	}
}

// OpenTenantConnWithToken opens a connection to the tenant cluster using the
// token-based authentication during connection migration.
func (c *connector) OpenTenantConnWithToken(
	ctx context.Context, requester balancer.ConnectionHandle, token string,
) (retServerConn net.Conn, retErr error) {
	c.StartupMsg.Parameters[sessionRevivalTokenStartupParam] = token
	defer func() {
		// Delete token after return.
		delete(c.StartupMsg.Parameters, sessionRevivalTokenStartupParam)
	}()

	serverConn, err := c.dialTenantCluster(ctx, requester)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = serverConn.Close()
		}
	}()

	// When we use token-based authentication, we will still get the initial
	// connection data messages (e.g. ParameterStatus and BackendKeyData).
	// Since this method is only used during connection migration (i.e. proxy
	// is connecting to the SQL pod), we'll discard all the messages, and
	// only return once we've seen a ReadyForQuery message.
	newBackendKeyData, err := readTokenAuthResult(serverConn)
	if err != nil {
		return nil, err
	}
	c.CancelInfo.setNewBackend(newBackendKeyData, serverConn.RemoteAddr().(*net.TCPAddr))
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
	ctx context.Context,
	requester balancer.ConnectionHandle,
	clientConn net.Conn,
	throttleHook func(throttler.AttemptStatus) error,
) (retServerConnection net.Conn, sentToClient bool, retErr error) {
	// Just a safety check, but this shouldn't happen since we will block the
	// startup param in the frontend admitter. The only case where we actually
	// need to delete this param is if OpenTenantConnWithToken was called
	// previously, but that wouldn't happen based on the current proxy logic.
	delete(c.StartupMsg.Parameters, sessionRevivalTokenStartupParam)

	serverConn, err := c.dialTenantCluster(ctx, requester)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if retErr != nil {
			_ = serverConn.Close()
		}
	}()

	// Perform user authentication for non-token-based auth methods. This will
	// block until the server has authenticated the client.
	crdbBackendKeyData, err := authenticate(clientConn, serverConn, c.CancelInfo.proxyBackendKeyData, throttleHook)
	if err != nil {
		return nil, true, err
	}
	log.Infof(ctx, "connected to %s through normal auth", serverConn.RemoteAddr())
	c.CancelInfo.setNewBackend(crdbBackendKeyData, serverConn.RemoteAddr().(*net.TCPAddr))
	return serverConn, false, nil
}

// dialTenantCluster returns a connection to the tenant cluster associated
// with the connector. Once a connection has been established, the pgwire
// startup message will be relayed to the server.
func (c *connector) dialTenantCluster(
	ctx context.Context, requester balancer.ConnectionHandle,
) (net.Conn, error) {
	if c.testingKnobs.dialTenantCluster != nil {
		return c.testingKnobs.dialTenantCluster(ctx, requester)
	}

	if c.DialTenantLatency != nil {
		start := timeutil.Now()
		defer func() { c.DialTenantLatency.RecordValue(timeutil.Since(start).Nanoseconds()) }()
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

	isRetry := false
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		// Track the number of dial retries.
		if isRetry && c.DialTenantRetries != nil {
			c.DialTenantRetries.Inc(1)
		}
		isRetry = true

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
		//
		// TODO(jaylim-crl): See comment above about moving lookupAddr into
		// the balancer.
		serverAssignment := balancer.NewServerAssignment(
			c.TenantID, c.Balancer.GetTracker(), requester, serverAddr,
		)
		crdbConn, err = c.dialSQLServer(ctx, serverAssignment)
		if err != nil {
			// Clean up the server assignment in case of an error. If there
			// was no error, the cleanup process is merged with net.Conn.Close().
			serverAssignment.Close()

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
				if reportErr := reportFailureToDirectoryCache(
					ctx, c.TenantID, serverAssignment.Addr(), c.DirectoryCache,
				); reportErr != nil {
					reportFailureErrs++
					if reportFailureErr.ShouldLog() {
						log.Ops.Errorf(ctx,
							"report failure (%d errors skipped): %v",
							reportFailureErrs,
							reportErr,
						)
						reportFailureErrs = 0
					}
					// nolint:errwrap
					err = errors.Wrapf(err, "reporting failure: %s", reportErr.Error())
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
	// ctx.Err() != nil).
	if err == nil || ctx.Err() == nil {
		// nolint:errwrap
		return nil, errors.AssertionFailedf(
			"unexpected retry loop exit, err=%v, ctxErr=%v",
			err,
			ctx.Err(),
		)
	}
	// If the error is already marked, just return that.
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return nil, err
	}
	// Otherwise, mark the error, and return that.
	return nil, errors.Mark(err, ctx.Err())
}

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

	// Lookup tenant in the directory cache. Once we have retrieve the list of
	// pods, use the Balancer for load balancing.
	pods, err := c.DirectoryCache.LookupTenantPods(ctx, c.TenantID)
	switch {
	case err == nil:
		runningPods := make([]*tenant.Pod, 0, len(pods))
		for _, pod := range pods {
			if pod.State == tenant.RUNNING {
				runningPods = append(runningPods, pod)
			}
		}
		pod, err := c.Balancer.SelectTenantPod(runningPods)
		if err != nil {
			// This should never happen because LookupTenantPods ensured that
			// there should be at least one RUNNING pod. Mark it as a retriable
			// connection anyway.
			return "", markAsRetriableConnectorError(err)
		}
		return pod.Addr, nil

	case status.Code(err) == codes.FailedPrecondition:
		if st, ok := status.FromError(err); ok {
			return "", withCode(errors.Newf("%v", st.Message()), codeUnavailable)
		}
		return "", withCode(errors.New("unavailable"), codeUnavailable)

	case status.Code(err) == codes.NotFound:
		return "", withCode(
			errors.Newf("cluster %s-%d not found", c.ClusterName, c.TenantID.ToUint64()),
			codeParamsRoutingFailed)

	default:
		return "", markAsRetriableConnectorError(err)
	}
}

// dialSQLServer dials a SQL pod based on the given server assignment, and
// forwards the startup message to the server. If the connector specifies a TLS
// connection, it will also attempt to upgrade the PG connection to use TLS.
//
// This will be called within an infinite backoff loop. If an error is
// transient, this will return an error that has been marked with
// errRetryConnectorSentinel (i.e. markAsRetriableConnectorError).
func (c *connector) dialSQLServer(
	ctx context.Context, serverAssignment *balancer.ServerAssignment,
) (_ net.Conn, retErr error) {
	if c.testingKnobs.dialSQLServer != nil {
		return c.testingKnobs.dialSQLServer(serverAssignment)
	}

	var tlsConf *tls.Config
	if c.TLSConfig != nil {
		var err error
		if tlsConf, err = tlsConfigForTenant(c.TenantID, serverAssignment.Addr(), c.TLSConfig); err != nil {
			return nil, err
		}
	}

	// TODO(JeffSwenson): The five second time out is pretty mediocre. It's too
	// short if the sql server is overloaded and too long if everything is
	// working the way it should. Ideally the fixed the timeout would be replaced
	// by an adaptive timeout or maybe speculative retries on a different server.
	var conn net.Conn
	err := timeutil.RunWithTimeout(ctx, "backend-dial", time.Second*5, func(ctx context.Context) error {
		var err error
		conn, err = BackendDial(ctx, c.StartupMsg, serverAssignment.Addr(), tlsConf)
		return err
	})
	if err != nil {
		if getErrorCode(err) == codeBackendDialFailed {
			return nil, markAsRetriableConnectorError(err)
		}
		return nil, err
	}

	// Add a connection wrapper that annotates errors as belonging to the sql
	// server.
	conn = &errorSourceConn{
		Conn:           conn,
		readErrMarker:  errServerRead,
		writeErrMarker: errServerWrite,
	}

	// Add a connection wrapper that lets the balancer know the connection is
	// closed.
	conn = &onConnectionClose{
		Conn:     conn,
		closerFn: serverAssignment.Close,
	}

	return conn, nil
}

// onConnectionClose is a net.Conn wrapper to ensure that our custom closerFn
// gets invoked whenever Close is called. closerFn must be idempotent.
type onConnectionClose struct {
	net.Conn
	closerFn func()
}

// Close invokes our custom closer function before closing the underlying
// net.Conn instance.
//
// Close implements the net.Conn interface.
func (cc *onConnectionClose) Close() error {
	cc.closerFn()
	return cc.Conn.Close()
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

// tlsConfigForTenant customizes the tls configuration for connecting to a
// specific tenant's sql server. Tenant certificates have two key features:
//
//  1. OU=Tenant
//  2. CommonName=<tenant_id>
//
// The certificate is also expected to have a dns san that matches the
// sqlServerAddr.
func tlsConfigForTenant(
	tenantID roachpb.TenantID, sqlServerAddr string, baseConfig *tls.Config,
) (*tls.Config, error) {
	config := baseConfig.Clone()

	// serverAssignment.Addr() will always have a port. We use an empty
	// string as the default port as we only care about extracting the host.
	outgoingHost, _, err := addr.SplitHostPort(sqlServerAddr, "" /* defaultPort */)
	if err != nil {
		return nil, err
	}

	// Always set ServerName. If InsecureSkipVerify is true, this will be
	// ignored.
	config.ServerName = outgoingHost

	config.VerifyConnection = func(state tls.ConnectionState) error {
		if config.InsecureSkipVerify {
			return nil
		}
		if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
			// This should never happen. VerifyConnection is only called if the
			// server provided a cert and the cert's CA was valid.
			return errors.AssertionFailedf("VerifyConnection called with no verified chains")
		}
		serverCert := state.VerifiedChains[0][0]

		// TODO(jeffswenson): once URI SANs are added to the tenant sql
		// servers, this should validate the URI SAN.
		if !security.IsTenantCertificate(serverCert) {
			return errors.Newf("%s's certificate is not a tenant cert", outgoingHost)
		}
		if serverCert.Subject.CommonName != tenantID.String() {
			return errors.Newf("expected a cert for tenant %d found '%s'", tenantID, serverCert.Subject.CommonName)
		}
		return nil
	}

	return config, nil
}
