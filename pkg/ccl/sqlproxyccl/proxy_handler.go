// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/balancer"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/denylist"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenantdirsvr"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/certmgr"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/jackc/pgproto3/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	// This assumes that whitespaces are used to separate command line args.
	// Unlike the original spec, this does not handle escaping rules.
	//
	// See "options" in https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS.
	clusterIdentifierLongOptionRE = regexp.MustCompile(`(?:-c\s*|--)cluster=([\S]*)`)

	// clusterNameRegex restricts cluster names to have between 6 and 100
	// alphanumeric characters, with dashes allowed within the name (but not as
	// a starting or ending character).
	//
	// Note that the limit for cluster names within CockroachCloud is likely
	// smaller than 100 characters. We don't perform an exact match here for
	// more flexibility.
	clusterNameRegex = regexp.MustCompile("^[a-z0-9][a-z0-9-]{4,98}[a-z0-9]$")
)

const (
	// Cluster identifier is in the form "<cluster name>-<tenant id>. Tenant ID
	// is always in the end but the cluster name can also contain '-' or digits.
	// (e.g. In "foo-7-10", cluster name is "foo-7" and tenant ID is "10")
	clusterTenantSep = "-"
)

// ProxyOptions is the information needed to construct a new proxyHandler.
type ProxyOptions struct {
	// Denylist file to limit access to IP addresses and tenant ids.
	Denylist string
	// ListenAddr is the listen address for incoming connections.
	ListenAddr string
	// ListenCert is the file containing PEM-encoded x509 certificate for listen
	// address. Set to "*" to auto-generate self-signed cert.
	ListenCert string
	// ListenKey is the file containing PEM-encoded x509 key for listen address.
	// Set to "*" to auto-generate self-signed cert.
	ListenKey string
	// MetricsAddress is the listen address for incoming connections for metrics
	// retrieval.
	MetricsAddress string
	// SkipVerify if set will skip the identity verification of the
	// backend. This is for testing only.
	SkipVerify bool
	// Insecure if set, will not use TLS for the backend connection. For testing.
	Insecure bool
	// RoutingRule for constructing the backend address for each incoming
	// connection.
	//
	// TODO(jaylim-crl): Rename RoutingRule to TestRoutingRule to be
	// explicit that this is only used in a testing environment.
	RoutingRule string
	// DirectoryAddr specified optional {HOSTNAME}:{PORT} for service that does
	// the resolution from backend id to IP address. If specified - it will be
	// used instead of the routing rule above.
	DirectoryAddr string
	// RatelimitBaseDelay is the initial backoff after a failed login attempt.
	// Set to 0 to disable rate limiting.
	RatelimitBaseDelay time.Duration
	// ValidateAccessInterval is the time interval between validations, confirming
	// that current connections are still valid.
	ValidateAccessInterval time.Duration
	// PollConfigInterval defines polling interval for pickup up changes in
	// config file.
	PollConfigInterval time.Duration
	// ThrottleBaseDelay is the initial exponential backoff triggered in
	// response to the first connection failure.
	ThrottleBaseDelay time.Duration
	// DisableConnectionRebalancing disables connection rebalancing for tenants.
	DisableConnectionRebalancing bool

	// testingKnobs are knobs used for testing.
	testingKnobs struct {
		// dirOpts is used to customize the directory cache created by the
		// proxy.
		dirOpts []tenant.DirOption

		// directoryServer represents the directory server that will be used
		// by the proxy handler. If unset, initializing the proxy handler will
		// create one, and populate this value.
		directoryServer tenant.DirectoryServer

		// balancerOpts is used to customize the balancer created by the proxy.
		balancerOpts []balancer.Option

		httpCancelErrHandler func(err error)
	}
}

// proxyHandler is the default implementation of a proxy handler.
type proxyHandler struct {
	ProxyOptions

	// metrics contains various counters reflecting the proxy operations.
	metrics *metrics

	// stopper is used to do an orderly shutdown.
	stopper *stop.Stopper

	// incomingCert is the managed cert of the proxy endpoint to
	// which clients connect.
	incomingCert certmgr.Cert

	// denyListWatcher provides access control.
	denyListWatcher *denylist.Watcher

	// throttleService will do throttling of incoming connection requests.
	throttleService throttler.Service

	// directoryCache is used to resolve tenants to their IP addresses.
	directoryCache tenant.DirectoryCache

	// balancer is used to load balance incoming connections.
	balancer *balancer.Balancer

	// certManager keeps up to date the certificates used.
	certManager *certmgr.CertManager

	// cancelInfoMap keeps track of all the cancel request keys for this proxy.
	cancelInfoMap *cancelInfoMap
}

const throttledErrorHint string = `Connection throttling is triggered by repeated authentication failure. Make
sure the username and password are correct.
`

var throttledError = errors.WithHint(
	withCode(errors.New(
		"connection attempt throttled"), codeProxyRefusedConnection),
	throttledErrorHint)

// newProxyHandler will create a new proxy handler with configuration based on
// the provided options.
func newProxyHandler(
	ctx context.Context,
	stopper *stop.Stopper,
	registry *metric.Registry,
	proxyMetrics *metrics,
	options ProxyOptions,
) (*proxyHandler, error) {
	ctx, _ = stopper.WithCancelOnQuiesce(ctx)

	handler := proxyHandler{
		stopper:       stopper,
		metrics:       proxyMetrics,
		ProxyOptions:  options,
		certManager:   certmgr.NewCertManager(ctx),
		cancelInfoMap: makeCancelInfoMap(),
	}

	err := handler.setupIncomingCert(ctx)
	if err != nil {
		return nil, err
	}

	// If denylist functionality is requested, create the denylist service.
	if options.Denylist != "" {
		handler.denyListWatcher = denylist.WatcherFromFile(ctx, options.Denylist,
			denylist.WithPollingInterval(options.PollConfigInterval))
	} else {
		handler.denyListWatcher = denylist.NilWatcher()
	}

	handler.throttleService = throttler.NewLocalService(
		throttler.WithBaseDelay(handler.ThrottleBaseDelay),
	)

	// TODO(jaylim-crl): Clean up how we start different types of directory
	// servers. We could have two options: remote or local. Local servers that
	// are using the in-memory implementation should only be used for testing
	// only. For production use-cases, we will need to update that to listen
	// on an actual network address, but there are no plans to support that at
	// the moment.
	var conn *grpc.ClientConn
	if handler.testingKnobs.directoryServer != nil {
		// TODO(jaylim-crl): For now, only support the static version. We should
		// make this part of a LocalDirectoryServer interface for us to grab the
		// in-memory listener.
		directoryServer, ok := handler.testingKnobs.directoryServer.(*tenantdirsvr.TestStaticDirectoryServer)
		if !ok {
			return nil, errors.New("unsupported test directory server")
		}
		conn, err = grpc.DialContext(
			ctx,
			"",
			grpc.WithContextDialer(directoryServer.DialerFunc),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
	} else if handler.DirectoryAddr != "" {
		conn, err = grpc.Dial(
			handler.DirectoryAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
	} else {
		// If no directory address was specified, assume routing rule, and
		// start an in-memory simple directory server.
		directoryServer, grpcServer := tenantdirsvr.NewTestSimpleDirectoryServer(handler.RoutingRule)
		ln, err := tenantdirsvr.ListenAndServeInMemGRPC(ctx, stopper, grpcServer)
		if err != nil {
			return nil, err
		}
		handler.testingKnobs.directoryServer = directoryServer

		dialerFunc := func(ctx context.Context, addr string) (net.Conn, error) {
			return ln.DialContext(ctx)
		}
		conn, err = grpc.DialContext(
			ctx,
			"",
			grpc.WithContextDialer(dialerFunc),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
	}
	stopper.AddCloser(stop.CloserFn(func() {
		_ = conn.Close() // nolint:grpcconnclose
	}))

	var dirOpts []tenant.DirOption
	podWatcher := make(chan *tenant.Pod)
	dirOpts = append(dirOpts, tenant.PodWatcher(podWatcher))
	if handler.testingKnobs.dirOpts != nil {
		dirOpts = append(dirOpts, handler.testingKnobs.dirOpts...)
	}

	client := tenant.NewDirectoryClient(conn)
	handler.directoryCache, err = tenant.NewDirectoryCache(ctx, stopper, client, dirOpts...)
	if err != nil {
		return nil, err
	}

	balancerMetrics := balancer.NewMetrics()
	registry.AddMetricStruct(balancerMetrics)
	var balancerOpts []balancer.Option
	if handler.DisableConnectionRebalancing {
		balancerOpts = append(balancerOpts, balancer.DisableRebalancing())
	}
	if handler.testingKnobs.balancerOpts != nil {
		balancerOpts = append(balancerOpts, handler.testingKnobs.balancerOpts...)
	}
	handler.balancer, err = balancer.NewBalancer(ctx, stopper, balancerMetrics, handler.directoryCache, balancerOpts...)
	if err != nil {
		return nil, err
	}

	// Only start the pod watcher once everything has been initialized. This
	// will depend on the balancer eventually.
	go handler.startPodWatcher(ctx, podWatcher)

	return &handler, nil
}

// handle is called by the proxy server to handle a single incoming client
// connection.
func (handler *proxyHandler) handle(ctx context.Context, incomingConn net.Conn) error {
	connRecievedTime := timeutil.Now()

	fe := FrontendAdmit(incomingConn, handler.incomingTLSConfig())
	defer func() { _ = fe.Conn.Close() }()
	if fe.Err != nil {
		SendErrToClient(fe.Conn, fe.Err)
		return fe.Err
	}

	// Cancel requests are sent on a separate connection, and have no response,
	// so we can close the connection immediately, then handle the request. This
	// prevents the client from using latency to learn if we are processing the
	// request or not.
	if cr := fe.CancelRequest; cr != nil {
		_ = incomingConn.Close()
		if err := handler.handleCancelRequest(cr, true /* allowForward */); err != nil {
			// Lots of noise from this log indicates that somebody is spamming
			// fake cancel requests.
			log.Warningf(
				ctx, "could not handle cancel request from client %s: %v",
				incomingConn.RemoteAddr().String(), err,
			)
		}
		return nil
	}

	// This should not happen. Return nil to the server, which simply closes the
	// connection.
	if fe.Msg == nil {
		return nil
	}

	// NOTE: Errors returned from this function are user-facing errors so we
	// should be careful with the details that we want to expose.
	backendStartupMsg, clusterName, tenID, err := clusterNameAndTenantFromParams(ctx, fe)
	if err != nil {
		clientErr := withCode(err, codeParamsRoutingFailed)
		log.Errorf(ctx, "unable to extract cluster name and tenant id: %s", err.Error())
		updateMetricsAndSendErrToClient(clientErr, fe.Conn, handler.metrics)
		return clientErr
	}

	ctx = logtags.AddTag(ctx, "cluster", clusterName)
	ctx = logtags.AddTag(ctx, "tenant", tenID)

	// Use an empty string as the default port as we only care about the
	// correctly parsing the IP address here.
	ipAddr, _, err := addr.SplitHostPort(fe.Conn.RemoteAddr().String(), "")
	if err != nil {
		clientErr := withCode(errors.New(
			"unexpected connection address"), codeParamsRoutingFailed)
		log.Errorf(ctx, "could not parse address: %v", err.Error())
		updateMetricsAndSendErrToClient(clientErr, fe.Conn, handler.metrics)
		return clientErr
	}

	errConnection := make(chan error, 1)

	removeListener, err := handler.denyListWatcher.ListenForDenied(
		denylist.ConnectionTags{IP: ipAddr, Cluster: tenID.String()},
		func(err error) {
			err = withCode(errors.Wrap(err,
				"connection added to deny list"), codeExpiredClientConnection)
			select {
			case errConnection <- err: /* error reported */
			default: /* the channel already contains an error */
			}
		},
	)
	if err != nil {
		log.Errorf(ctx, "connection matched denylist: %v", err)
		err = withCode(errors.New(
			"connection refused"), codeProxyRefusedConnection)
		updateMetricsAndSendErrToClient(err, fe.Conn, handler.metrics)
		return err
	}
	defer removeListener()

	throttleTags := throttler.ConnectionTags{IP: ipAddr, TenantID: tenID.String()}
	throttleTime, err := handler.throttleService.LoginCheck(throttleTags)
	if err != nil {
		log.Errorf(ctx, "throttler refused connection: %v", err.Error())
		err = throttledError
		updateMetricsAndSendErrToClient(err, fe.Conn, handler.metrics)
		return err
	}

	connector := &connector{
		ClusterName:       clusterName,
		TenantID:          tenID,
		DirectoryCache:    handler.directoryCache,
		Balancer:          handler.balancer,
		StartupMsg:        backendStartupMsg,
		DialTenantLatency: handler.metrics.DialTenantLatency,
		DialTenantRetries: handler.metrics.DialTenantRetries,
		CancelInfo:        makeCancelInfo(incomingConn.LocalAddr(), incomingConn.RemoteAddr()),
	}

	// TLS options for the proxy are split into Insecure and SkipVerify.
	// In insecure mode, TLSConfig is expected to be nil. This will cause the
	// connector's dialer to skip TLS entirely. If SkipVerify is true,
	// TLSConfig will be set to a non-nil config with InsecureSkipVerify set
	// to true. InsecureSkipVerify will provide an encrypted connection but
	// not verify that the connection recipient is a trusted party.
	if !handler.Insecure {
		connector.TLSConfig = &tls.Config{InsecureSkipVerify: handler.SkipVerify}
	}

	f := newForwarder(ctx, connector, handler.metrics, nil /* timeSource */)
	defer f.Close()

	crdbConn, sentToClient, err := connector.OpenTenantConnWithAuth(ctx, f, fe.Conn,
		func(status throttler.AttemptStatus) error {
			if err := handler.throttleService.ReportAttempt(
				ctx, throttleTags, throttleTime, status,
			); err != nil {
				log.Errorf(ctx, "throttler refused connection after authentication: %v", err.Error())
				return throttledError
			}
			return nil
		},
	)
	if err != nil {
		log.Errorf(ctx, "could not connect to cluster: %v", err.Error())
		if sentToClient {
			handler.metrics.updateForError(err)
		} else {
			updateMetricsAndSendErrToClient(err, fe.Conn, handler.metrics)
		}
		return err
	}
	defer func() { _ = crdbConn.Close() }()

	// Update the cancel info.
	handler.cancelInfoMap.addCancelInfo(connector.CancelInfo.proxySecretID(), connector.CancelInfo)

	// Record the connection success and how long it took.
	handler.metrics.ConnectionLatency.RecordValue(timeutil.Since(connRecievedTime).Nanoseconds())
	handler.metrics.SuccessfulConnCount.Inc(1)

	log.Infof(ctx, "new connection")
	connBegin := timeutil.Now()
	defer func() {
		log.Infof(ctx, "closing after %.2fs", timeutil.Since(connBegin).Seconds())
		handler.cancelInfoMap.deleteCancelInfo(connector.CancelInfo.proxySecretID())
	}()

	// Wrap the client connection with an error annotater. WARNING: The TLS
	// wrapper must be inside the errorSourceConn and not the other way around.
	// The TLS connection attempts to cast errors to a net.Err and will behave
	// incorrectly if handed a marked error.
	clientConn := &errorSourceConn{
		Conn:           fe.Conn,
		readErrMarker:  errClientRead,
		writeErrMarker: errClientWrite,
	}

	// Pass ownership of conn and crdbConn to the forwarder.
	if err := f.run(clientConn, crdbConn); err != nil {
		// Don't send to the client here for the same reason below.
		handler.metrics.updateForError(err)
		return errors.Wrap(err, "running forwarder")
	}

	// Block until an error is received, or when the stopper starts quiescing,
	// whichever that happens first.
	//
	// TODO(jaylim-crl): We should handle all these errors properly, and
	// propagate them back to the client if we're in a safe position to send.
	// This PR https://github.com/cockroachdb/cockroach/pull/66205 removed error
	// injections after connection handoff because there was a possibility of
	// corrupted packets.
	//
	// TODO(jaylim-crl): It would be nice to have more consistency in how we
	// manage background goroutines, communicate errors, etc.
	select {
	case <-ctx.Done():
		// Context cancellations do not terminate forward() so we will need
		// to manually handle that here. When this returns, we would call
		// f.Close(). This should only happen during shutdown.
		handler.metrics.updateForError(ctx.Err())
		return ctx.Err()
	case err := <-f.errCh: // From forwarder.
		handler.metrics.updateForError(err)
		return err
	case err := <-errConnection: // From denyListWatcher.
		handler.metrics.updateForError(err)
		return err
	case <-handler.stopper.ShouldQuiesce():
		err := context.Canceled
		handler.metrics.updateForError(err)
		return err
	}
}

// handleCancelRequest handles a pgwire query cancel request by either
// forwarding it to a SQL node or to another proxy.
func (handler *proxyHandler) handleCancelRequest(
	cr *proxyCancelRequest, allowForward bool,
) (retErr error) {
	if allowForward {
		handler.metrics.QueryCancelReceivedPGWire.Inc(1)
	} else {
		handler.metrics.QueryCancelReceivedHTTP.Inc(1)
	}
	var triedForward bool
	defer func() {
		if retErr != nil {
			handler.metrics.QueryCancelIgnored.Inc(1)
		} else if triedForward {
			handler.metrics.QueryCancelForwarded.Inc(1)
		} else {
			handler.metrics.QueryCancelSuccessful.Inc(1)
		}
	}()
	if ci, ok := handler.cancelInfoMap.getCancelInfo(cr.SecretKey); ok {
		return ci.sendCancelToBackend(cr.ClientIP)
	}
	// Only forward the request if it hasn't already been sent to the correct proxy.
	if !allowForward {
		return errors.Newf("ignoring cancel request with unfamiliar key: %d", cr.SecretKey)
	}
	triedForward = true
	u := "http://" + cr.ProxyIP.String() + ":8080/_status/cancel/"
	reqBody := bytes.NewReader(cr.Encode())
	return forwardCancelRequest(u, reqBody)
}

var forwardCancelRequest = func(url string, reqBody *bytes.Reader) error {
	const timeout = 2 * time.Second
	client := http.Client{
		Timeout: timeout,
	}

	if _, err := client.Post(url, "application/octet-stream", reqBody); err != nil {
		return err
	}
	return nil
}

// startPodWatcher runs on a background goroutine and listens to pod change
// notifications. When a pod transitions into the DRAINING state, a rebalance
// operation will be attempted for that particular pod's tenant.
func (handler *proxyHandler) startPodWatcher(ctx context.Context, podWatcher chan *tenant.Pod) {
	for {
		select {
		case <-ctx.Done():
			return
		case pod := <-podWatcher:
			// For better responsiveness, we only care about RUNNING pods.
			// DRAINING pods can wait until the next rebalance tick.
			//
			// Note that there's no easy way to tell whether a SQL pod is new
			// since this may race with fetchPodsLocked in tenantEntry, so we
			// will just attempt to rebalance whenever we see a RUNNING pod
			// event. In most cases, this should only occur when a new SQL pod
			// gets added (i.e. stamped), or a DRAINING pod transitions to a
			// RUNNING pod.
			if pod.State == tenant.RUNNING && pod.TenantID != 0 {
				handler.balancer.RebalanceTenant(ctx, roachpb.MustMakeTenantID(pod.TenantID))
			}
		}
	}
}

// incomingTLSConfig gets back the current TLS config for the incoming client
// connection endpoint.
func (handler *proxyHandler) incomingTLSConfig() *tls.Config {
	if handler.incomingCert == nil {
		return nil
	}

	cert := handler.incomingCert.TLSCert()
	if cert == nil {
		return nil
	}

	return &tls.Config{Certificates: []tls.Certificate{*cert}}
}

// setupIncomingCert will setup a managed cert for the incoming connections.
// They can either be unencrypted (in case a cert and key names are empty),
// using self-signed, runtime generated cert (if cert is set to *) or
// using file based cert where the cert/key values refer to file names
// containing the information.
func (handler *proxyHandler) setupIncomingCert(ctx context.Context) error {
	if (handler.ListenKey == "") != (handler.ListenCert == "") {
		return errors.New("must specify either both or neither of cert and key")
	}

	if handler.ListenCert == "" {
		return nil
	}

	// TODO(darin): change the cert manager so it uses the stopper.
	certMgr := certmgr.NewCertManager(ctx)
	var cert certmgr.Cert
	if handler.ListenCert == "*" {
		cert = certmgr.NewSelfSignedCert(0, 3, 0, 0)
	} else if handler.ListenCert != "" {
		cert = certmgr.NewFileCert(handler.ListenCert, handler.ListenKey)
	}
	cert.Reload(ctx)
	err := cert.Err()
	if err != nil {
		return err
	}
	certMgr.ManageCert("client", cert)
	handler.certManager = certMgr
	handler.incomingCert = cert

	return nil
}

// clusterNameAndTenantFromParams extracts the cluster name and tenant ID from
// the connection parameters, and rewrites the database and options parameters,
// if necessary.
//
// We currently support embedding the cluster identifier in three ways:
//
//   - Through server name identification (SNI) when using TLS connections
//     (e.g. happy-koala-3.5xj.gcp-us-central1.cockroachlabs.cloud)
//
// - Within the database param (e.g. "happy-koala-3.defaultdb")
//
//   - Within the options param (e.g. "... --cluster=happy-koala-5 ...").
//     PostgreSQL supports three different ways to set a run-time parameter
//     through its command-line options, i.e. "-c NAME=VALUE", "-cNAME=VALUE", and
//     "--NAME=VALUE".
func clusterNameAndTenantFromParams(
	ctx context.Context, fe *FrontendAdmitInfo,
) (*pgproto3.StartupMessage, string, roachpb.TenantID, error) {
	clusterIdentifierDB, databaseName, err := parseDatabaseParam(fe.Msg.Parameters["database"])
	if err != nil {
		return fe.Msg, "", roachpb.MaxTenantID, err
	}

	clusterIdentifierOpt, newOptionsParam, err := parseOptionsParam(fe.Msg.Parameters["options"])
	if err != nil {
		return fe.Msg, "", roachpb.MaxTenantID, err
	}

	var clusterName string
	var tenID roachpb.TenantID
	// No cluster identifiers were specified.
	if clusterIdentifierDB == "" && clusterIdentifierOpt == "" {
		var clusterIdentifierSNI string
		if i := strings.Index(fe.SniServerName, "."); i >= 0 {
			clusterIdentifierSNI = fe.SniServerName[:i]
		}
		if clusterIdentifierSNI != "" {
			clusterName, tenID, err = parseClusterIdentifier(ctx, clusterIdentifierSNI)
			if err == nil {
				// Identifier provider via SNI is a bit different from the identifiers
				// provided via DB (with dot) or options. With SNI it is possible that
				// the end user doesn't want to use SNI as a cluster identifier and at
				// the same time the driver or client they are using may be sending SNI
				// info it anyway. We don't have a way to know if they intended to use
				// SNI for cluster identification or not. So SNI will be a source of
				// last resort for cluster identification, tried only when DB and Opts
				// are blank. If SNI doesn't parse as a valid cluster id, then we assume
				// that the end user didn't intend to pass cluster info through SNI and
				// show missing cluster identifier instead of invalid cluster identifier.
				return fe.Msg, clusterName, tenID, nil
			}
		}
		err := errors.New("missing cluster identifier")
		err = errors.WithHint(err, clusterIdentifierHint)
		return fe.Msg, "", roachpb.MaxTenantID, err
	}

	// Ambiguous cluster identifiers.
	if clusterIdentifierDB != "" && clusterIdentifierOpt != "" &&
		clusterIdentifierDB != clusterIdentifierOpt {
		err := errors.New("multiple different cluster identifiers provided")
		err = errors.WithHintf(err,
			"Is '%s' or '%s' the identifier for the cluster that you're connecting to?",
			clusterIdentifierDB, clusterIdentifierOpt)
		err = errors.WithHint(err, clusterIdentifierHint)
		return fe.Msg, "", roachpb.MaxTenantID, err
	}

	if clusterIdentifierDB != "" {
		clusterName, tenID, err = parseClusterIdentifier(ctx, clusterIdentifierDB)
	} else {
		clusterName, tenID, err = parseClusterIdentifier(ctx, clusterIdentifierOpt)
	}
	if err != nil {
		return fe.Msg, "", roachpb.MaxTenantID, err
	}

	// Make and return a copy of the startup msg so the original is not modified.
	// We will rewrite database and options in the new startup message.
	paramsOut := map[string]string{}
	for key, value := range fe.Msg.Parameters {
		if key == "database" {
			paramsOut[key] = databaseName
		} else if key == "options" {
			if newOptionsParam != "" {
				paramsOut[key] = newOptionsParam
			}
		} else {
			paramsOut[key] = value
		}
	}

	outMsg := &pgproto3.StartupMessage{
		ProtocolVersion: fe.Msg.ProtocolVersion,
		Parameters:      paramsOut,
	}
	return outMsg, clusterName, tenID, nil
}

// parseClusterIdentifier will parse an identifier received via DB, opts or SNI
// and extract the tenant cluster name and tenant ID.
func parseClusterIdentifier(
	ctx context.Context, clusterIdentifier string,
) (string, roachpb.TenantID, error) {
	sepIdx := strings.LastIndex(clusterIdentifier, clusterTenantSep)

	// Cluster identifier provided without a tenant ID in the end.
	if sepIdx == -1 || sepIdx == len(clusterIdentifier)-1 {
		err := errors.Errorf("invalid cluster identifier '%s'", clusterIdentifier)
		err = errors.WithHint(err, missingTenantIDHint)
		err = errors.WithHint(err, clusterNameFormHint)
		return "", roachpb.MaxTenantID, err
	}

	clusterName, tenantIDStr := clusterIdentifier[:sepIdx], clusterIdentifier[sepIdx+1:]

	// Cluster name does not conform to the expected format (e.g. too short).
	if !clusterNameRegex.MatchString(clusterName) {
		err := errors.Errorf("invalid cluster identifier '%s'", clusterIdentifier)
		err = errors.WithHintf(err, "Is '%s' a valid cluster name?", clusterName)
		err = errors.WithHint(err, clusterNameFormHint)
		return "", roachpb.MaxTenantID, err
	}

	// Tenant ID cannot be parsed.
	tenID, err := strconv.ParseUint(tenantIDStr, 10, 64)
	if err != nil {
		// Log these non user-facing errors.
		log.Errorf(ctx, "cannot parse tenant ID in %s: %v", clusterIdentifier, err)
		err := errors.Errorf("invalid cluster identifier '%s'", clusterIdentifier)
		err = errors.WithHintf(err, "Is '%s' a valid tenant ID?", tenantIDStr)
		err = errors.WithHint(err, clusterNameFormHint)
		return "", roachpb.MaxTenantID, err
	}

	// This case only happens if tenID is 0 or 1 (system tenant).
	if tenID < roachpb.MinTenantID.ToUint64() {
		// Log these non user-facing errors.
		log.Errorf(ctx, "%s contains an invalid tenant ID", clusterIdentifier)
		err := errors.Errorf("invalid cluster identifier '%s'", clusterIdentifier)
		err = errors.WithHintf(err, "Tenant ID %d is invalid.", tenID)
		return "", roachpb.MaxTenantID, err
	}

	return clusterName, roachpb.MustMakeTenantID(tenID), nil
}

// parseDatabaseParam parses the database parameter from the PG connection
// string, and tries to extract the cluster identifier if present. The cluster
// identifier should be embedded in the database parameter using the dot (".")
// delimiter in the form of "<cluster identifier>.<database name>". This
// approach is safe because dots are not allowed in the database names
// themselves.
func parseDatabaseParam(databaseParam string) (clusterIdentifier, databaseName string, err error) {
	// Database param is not provided.
	if databaseParam == "" {
		return "", "", nil
	}

	parts := strings.Split(databaseParam, ".")

	// Database param provided without cluster name.
	if len(parts) <= 1 {
		return "", databaseParam, nil
	}

	clusterIdentifier, databaseName = parts[0], parts[1]

	// Ensure that the param is in the right format if the delimiter is provided.
	if len(parts) > 2 || clusterIdentifier == "" || databaseName == "" {
		return "", "", errors.New("invalid database param")
	}

	return clusterIdentifier, databaseName, nil
}

// parseOptionsParam parses the options parameter from the PG connection string,
// and tries to return the cluster identifier if present. It also returns the
// options parameter with the cluster key stripped out. Just like PostgreSQL,
// the sqlproxy supports three different ways to set a run-time parameter
// through its command-line options:
//
//	-c NAME=VALUE (commonly used throughout documentation around PGOPTIONS)
//	-cNAME=VALUE
//	--NAME=VALUE
//
// Note that this parsing approach is not perfect as it allows a negative case
// like options="-c --cluster=happy-koala -c -c -c" to go through. To properly
// parse this, we need to traverse the string from left to right, and look at
// every single argument, but that involves quite a bit of work, so we'll punt
// for now.
func parseOptionsParam(optionsParam string) (clusterIdentifier, newOptionsParam string, err error) {
	// Only search up to 2 in case of large inputs.
	matches := clusterIdentifierLongOptionRE.FindAllStringSubmatch(optionsParam, 2 /* n */)
	if len(matches) == 0 {
		return "", optionsParam, nil
	}

	if len(matches) > 1 {
		// Technically we could still allow requests to go through if all
		// cluster identifiers match, but we don't want to parse the entire
		// string, so we will just error out if at least two cluster flags are
		// provided.
		return "", "", errors.New("multiple cluster flags provided")
	}

	// Length of each match should always be 2 with the given regex, one for
	// the full string, and the other for the cluster identifier.
	if len(matches[0]) != 2 {
		// We don't want to panic here.
		return "", "", errors.New("internal server error")
	}

	// Flag was provided, but value is NULL.
	if len(matches[0][1]) == 0 {
		return "", "", errors.New("invalid cluster flag")
	}

	newOptionsParam = strings.ReplaceAll(optionsParam, matches[0][0], "")
	newOptionsParam = strings.TrimSpace(newOptionsParam)
	return matches[0][1], newOptionsParam, nil
}

const clusterIdentifierHint = `Ensure that your cluster identifier is uniquely specified using any of the
following methods:

1) Host name:
   Use a driver that supports server name identification (SNI) with TLS 
   connection and the hostname assigned to your cluster 
   (e.g. serverless-101.5xj.gcp-us-central1.cockroachlabs.cloud)

2) Options parameter:
   Use "--cluster=<cluster identifier>" as the options parameter.
   (e.g. options="--cluster=active-roach-42")

3) Database parameter:
   Use "<cluster identifier>.<database name>" as the database parameter.
   (e.g. database="active-roach-42.defaultdb")

For more details, please visit our docs site at:
	https://www.cockroachlabs.com/docs/cockroachcloud/connect-to-a-serverless-cluster
`

const clusterNameFormHint = "Cluster identifiers come in the form of <name>-<tenant ID> (e.g. lazy-roach-3)."

const missingTenantIDHint = "Did you forget to include your tenant ID in the cluster identifier?"
