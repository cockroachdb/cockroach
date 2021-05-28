// Copyright 2021 The Cockroach Authors.
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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/cache"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/denylist"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/idle"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/certmgr"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/jackc/pgproto3/v2"
	"google.golang.org/grpc"
)

var (
	// This assumes that whitespaces are used to separate command line args.
	// Unlike the original spec, this does not handle escaping rules.
	//
	// See "options" in https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS.
	clusterNameLongOptionRE = regexp.MustCompile(`(?:-c\s*|--)cluster=([\S]*)`)

	// clusterNameRegex restricts cluster names to have between 6 and 20
	// alphanumeric characters, with dashes allowed within the name (but not as a
	// starting or ending character).
	clusterNameRegex = regexp.MustCompile("^[a-z0-9][a-z0-9-]{4,18}[a-z0-9]$")
)

const (
	// Cluster identifier is in the form "clustername-<tenant_id>. Tenant id is
	// always in the end but the cluster name can also contain '-' or  digits.
	// For example:
	// "foo-7-10" -> cluster name is "foo-7" and tenant id is 10.
	clusterTenantSep = "-"
	// TODO(spaskob): add ballpark estimate.
	maxKnownConnCacheSize = 5e6 // 5 million.
)

// ProxyOptions is the information needed to construct a new proxyHandler.
type ProxyOptions struct {
	// Denylist file to limit access to IP addresses and tenant ids.
	Denylist string
	// ListenAddr is the listen address for incoming connections.
	ListenAddr string
	// ListenCert is the file containing PEM-encoded x509 certificate for listen address.
	// Set to "*" to auto-generate self-signed cert.
	ListenCert string
	// ListenKey is the file containing PEM-encoded x509 key for listen address.
	// Set to "*" to auto-generate self-signed cert.
	ListenKey string
	// MetricsAddress is the listen address for incoming connections for metrics retrieval.
	MetricsAddress string
	// SkipVerify if set will skip the identity verification of the
	// backend. This is for testing only.
	SkipVerify bool
	// Insecure if set, will not use TLS for the backend connection. For testing.
	Insecure bool
	// RoutingRule for constructing the backend address for each incoming
	// connection. Optionally use '{{clusterName}}'
	// which will be substituted with the cluster name.
	RoutingRule string
	// DirectoryAddr specified optional {HOSTNAME}:{PORT} for service that does the resolution
	// from backend id to IP address. If specified - it will be used instead of the
	// routing rule above.
	DirectoryAddr string
	// RatelimitBaseDelay is the initial backoff after a failed login attempt.
	// Set to 0 to disable rate limiting.
	RatelimitBaseDelay time.Duration
	// ValidateAccessInterval is the time interval between validations, confirming
	// that current connections are still valid.
	ValidateAccessInterval time.Duration
	// PollConfigInterval defines polling interval for pickup up changes in config file.
	PollConfigInterval time.Duration
	// IdleTimeout if set, will close connections that have been idle for that duration.
	IdleTimeout time.Duration
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

	// denyListService provides access control.
	denyListService denylist.Service

	// throttleService will do throttling of incoming connection requests.
	throttleService throttler.Service

	// directory is optional and if set, will be used to resolve
	// backend id to IP addresses.
	directory *tenant.Directory

	// CertManger keeps up to date the certificates used.
	certManager *certmgr.CertManager

	//connCache is used to keep track of all current connections.
	connCache cache.ConnCache
}

// newProxyHandler will create a new proxy handler with configuration based on
// the provided options.
func newProxyHandler(
	ctx context.Context, stopper *stop.Stopper, proxyMetrics *metrics, options ProxyOptions,
) (*proxyHandler, error) {
	handler := proxyHandler{
		stopper:      stopper,
		metrics:      proxyMetrics,
		ProxyOptions: options,
		certManager:  certmgr.NewCertManager(ctx),
	}

	var err error
	err = handler.setupIncomingCert()
	if err != nil {
		return nil, err
	}

	ctx, _ = stopper.WithCancelOnQuiesce(ctx)
	handler.denyListService, err = denylist.NewViperDenyListFromFile(ctx, options.Denylist, options.PollConfigInterval)
	if err != nil {
		return nil, err
	}

	handler.throttleService = throttler.NewLocalService(throttler.WithBaseDelay(options.RatelimitBaseDelay))
	handler.connCache = cache.NewCappedConnCache(maxKnownConnCacheSize)

	if handler.DirectoryAddr != "" {
		conn, err := grpc.Dial(handler.DirectoryAddr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		// nolint:grpcconnclose
		stopper.AddCloser(stop.CloserFn(func() { _ = conn.Close() /* nolint:grpcconnclose */ }))
		client := tenant.NewDirectoryClient(conn)
		handler.directory, err = tenant.NewDirectory(ctx, stopper, client)
		if err != nil {
			return nil, err
		}
	}

	return &handler, nil
}

// handle is called by the proxy server to handle a single incoming client
// connection.
func (handler *proxyHandler) handle(ctx context.Context, proxyConn *conn) error {
	conn, msg, err := frontendAdmit(proxyConn, handler.incomingTLSConfig())
	defer func() { _ = conn.Close() }()
	if err != nil {
		sendErrToClient(conn, err)
		return err
	}

	// This currently only happens for CancelRequest type of startup messages
	// that we don't support
	if msg == nil {
		return nil
	}

	// Note that the errors returned from this function are user-facing errors so
	// we should be careful with the details that we want to expose.
	backendStartupMsg, clusterName, tenID, err := clusterNameAndTenantFromParams(msg)
	if err != nil {
		clientErr := &codeError{codeParamsRoutingFailed, err}
		log.Errorf(ctx, "unable to extract cluster name and tenant id: %s", clientErr.Error())
		updateMetricsAndSendErrToClient(clientErr, conn, handler.metrics)
		return clientErr
	}
	// This forwards the remote addr to the backend.
	backendStartupMsg.Parameters["crdb:remote_addr"] = conn.RemoteAddr().String()

	ctx = logtags.AddTag(ctx, "cluster", clusterName)
	ctx = logtags.AddTag(ctx, "tenant", tenID)

	ipAddr, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		clientErr := &codeError{codeParamsRoutingFailed, err}
		log.Errorf(ctx, "could not parse address: %v", clientErr.Error())
		updateMetricsAndSendErrToClient(clientErr, conn, handler.metrics)
		return clientErr
	}

	if err = handler.validateAccessAndThrottle(ctx, tenID, ipAddr); err != nil {
		updateMetricsAndSendErrToClient(err, conn, handler.metrics)
		return err
	}

	var TLSConf *tls.Config
	if !handler.Insecure {
		TLSConf = &tls.Config{InsecureSkipVerify: handler.SkipVerify}
	}

	var crdbConn net.Conn
	var outgoingAddress string
	retryOpts := retry.Options{InitialBackoff: 10 * time.Millisecond, MaxBackoff: time.Second}
	outgoingAddressErr := log.Every(time.Minute)
	backendDialErr := log.Every(time.Minute)
	reportFailureErr := log.Every(time.Minute)
	var outgoingAddressErrs, codeBackendDownErrs, reportFailureErrs int
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		outgoingAddress, err = handler.outgoingAddress(ctx, clusterName, tenID)
		if err != nil {
			if err.Error() != "not found" {
				outgoingAddressErrs++
				if outgoingAddressErr.ShouldLog() {
					log.Ops.Errorf(ctx,
						"outgoing address (%d errors skipped): %v",
						outgoingAddressErrs,
						err,
					)
					outgoingAddressErrs = 0
				}
				continue
			}
			clientErr := newErrorf(codeParamsRoutingFailed, "cluster %s-%d not found", clusterName, tenID.ToUint64())
			updateMetricsAndSendErrToClient(clientErr, conn, handler.metrics)
			break
		}

		crdbConn, err = backendDial(backendStartupMsg, outgoingAddress, TLSConf)
		// If we get a backend down error and are using the directory - report the
		// error to the directory and retry the connection.
		codeErr := (*codeError)(nil)
		if err != nil &&
			errors.As(err, &codeErr) &&
			codeErr.code == codeBackendDown &&
			handler.directory != nil {
			codeBackendDownErrs++
			if backendDialErr.ShouldLog() {
				log.Ops.Errorf(ctx,
					"backend dial (%d errors skipped): %v",
					codeBackendDownErrs,
					err,
				)
				codeBackendDownErrs = 0
			}
			err = handler.directory.ReportFailure(ctx, tenID, outgoingAddress)
			if err != nil {
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
		break
	}

	if err != nil {
		updateMetricsAndSendErrToClient(err, conn, handler.metrics)
		return err
	}

	crdbConn = idle.DisconnectOverlay(crdbConn, handler.IdleTimeout)

	defer func() { _ = crdbConn.Close() }()

	if err := authenticate(conn, crdbConn); err != nil {
		handler.metrics.updateForError(err)
		log.Ops.Errorf(ctx, "authenticate: %s", err)
		return err
	}

	handler.metrics.SuccessfulConnCount.Inc(1)

	handler.connCache.Insert(
		&cache.ConnKey{IPAddress: ipAddr, TenantID: tenID},
	)

	errConnectionCopy := make(chan error, 1)
	errExpired := make(chan error, 1)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// TODO(darinpp): starting a new go routine for every connection here is inefficient.
	// Change to maintain a map of connections by IP/tenant and have single
	// go routine that closes connections that are blocklisted.
	go func() {
		errExpired <- func(ctx context.Context) error {
			t := timeutil.NewTimer()
			defer t.Stop()
			t.Reset(0)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-t.C:
					t.Read = true
					if err := handler.validateAccess(ctx, tenID, ipAddr); err != nil {
						return err
					}
				}
				t.Reset(util.Jitter(handler.ValidateAccessInterval, 0.15))
			}
		}(ctx)
	}()

	log.Infof(ctx, "new connection")
	connBegin := timeutil.Now()
	defer func() {
		log.Infof(ctx, "closing after %.2fs", timeutil.Since(connBegin).Seconds())
	}()

	go func() {
		err := connectionCopy(crdbConn, conn)
		errConnectionCopy <- err
	}()

	select {
	case err := <-errConnectionCopy:
		updateMetricsAndSendErrToClient(err, conn, handler.metrics)
		return err
	case err := <-errExpired:
		if err != nil {
			// The client connection expired.
			codeErr := newErrorf(
				codeExpiredClientConnection, "expired client conn: %v", err,
			)
			updateMetricsAndSendErrToClient(codeErr, conn, handler.metrics)
			return codeErr
		}
		return nil
	case <-handler.stopper.ShouldQuiesce():
		return nil
	}

}

// outgoingAddress resolves a tenant ID and a tenant cluster name to an IP of
// the backend.
func (handler *proxyHandler) outgoingAddress(
	ctx context.Context, name string, tenID roachpb.TenantID,
) (string, error) {
	if handler.directory == nil {
		addr := strings.ReplaceAll(
			handler.RoutingRule, "{{clusterName}}", fmt.Sprintf("%s-%d", name, tenID.ToUint64()),
		)
		log.Infof(ctx, "backend %s resolved to '%s'", name, addr)
		return addr, nil
	}

	// This doesn't verify the name part of the tenant and relies just on the int id.
	addr, err := handler.directory.EnsureTenantIP(ctx, tenID, name)
	if err != nil {
		return "", err
	}
	return addr, nil
}

func (handler *proxyHandler) validateAccessAndThrottle(
	ctx context.Context, tenID roachpb.TenantID, ipAddr string,
) error {
	if err := handler.validateAccess(ctx, tenID, ipAddr); err != nil {
		return err
	}

	// Admit the connection
	connKey := cache.ConnKey{IPAddress: ipAddr, TenantID: tenID}
	if !handler.connCache.Exists(&connKey) {
		// Unknown previous successful connections from this IP and tenant.
		// Hence we need to rate limit.
		if err := handler.throttleService.LoginCheck(ipAddr, timeutil.Now()); err != nil {
			log.Errorf(ctx, "throttler refused connection: %v", err.Error())
			return newErrorf(codeProxyRefusedConnection, "Connection attempt throttled")
		}
	}

	return nil
}

func (handler *proxyHandler) validateAccess(
	ctx context.Context, tenID roachpb.TenantID, ipAddr string,
) error {
	// First validate against the deny list service
	list := handler.denyListService
	if entry, err := list.Denied(denylist.DenyEntity{Item: tenID.String(), Type: denylist.ClusterType}); err != nil {
		// Log error but don't return since this could be transient.
		log.Errorf(ctx, "could not consult denied list for tenant: %d", tenID.ToUint64())
	} else if entry != nil {
		log.Errorf(ctx, "access denied for tenant: %d, reason: %s", tenID.ToUint64(), entry.Reason)
		return newErrorf(codeProxyRefusedConnection, "tenant %d %s", tenID.ToUint64(), entry.Reason)
	}

	if entry, err := list.Denied(denylist.DenyEntity{Item: ipAddr, Type: denylist.IPAddrType}); err != nil {
		// Log error but don't return since this could be transient.
		log.Errorf(ctx, "could not consult denied list for IP address: %s", ipAddr)
	} else if entry != nil {
		log.Errorf(ctx, "access denied for IP address: %s, reason: %s", ipAddr, entry.Reason)
		return newErrorf(codeProxyRefusedConnection, "IP address %s %s", ipAddr, entry.Reason)
	}

	return nil
}

// clusterNameAndTenantFromParams extracts the cluster name from the connection
// parameters, and rewrites the database param, if necessary. We currently
// support embedding the cluster name in two ways:
// - Within the database param (e.g. "happy-koala.defaultdb")
//
// - Within the options param (e.g. "... --cluster=happy-koala ...").
//   PostgreSQL supports three different ways to set a run-time parameter
//   through its command-line options, i.e. "-c NAME=VALUE", "-cNAME=VALUE", and
//   "--NAME=VALUE".
func clusterNameAndTenantFromParams(
	msg *pgproto3.StartupMessage,
) (*pgproto3.StartupMessage, string, roachpb.TenantID, error) {
	clusterNameFromDB, databaseName, err := parseDatabaseParam(msg.Parameters["database"])
	if err != nil {
		return msg, "", roachpb.MaxTenantID, err
	}

	clusterNameFromOpt, err := parseOptionsParam(msg.Parameters["options"])
	if err != nil {
		return msg, "", roachpb.MaxTenantID, err
	}

	if clusterNameFromDB == "" && clusterNameFromOpt == "" {
		return msg, "", roachpb.MaxTenantID, errors.New("missing cluster name in connection string")
	}

	if clusterNameFromDB != "" && clusterNameFromOpt != "" {
		return msg, "", roachpb.MaxTenantID, errors.New("multiple cluster names provided")
	}

	if clusterNameFromDB == "" {
		clusterNameFromDB = clusterNameFromOpt
	}

	sepIdx := strings.LastIndex(clusterNameFromDB, clusterTenantSep)
	// Cluster name provided without a tenant ID in the end.
	if sepIdx == -1 || sepIdx == len(clusterNameFromDB)-1 {
		return msg, "", roachpb.MaxTenantID, errors.Errorf("invalid cluster name %s", clusterNameFromDB)
	}
	clusterNameSansTenant, tenantIDStr := clusterNameFromDB[:sepIdx], clusterNameFromDB[sepIdx+1:]

	if !clusterNameRegex.MatchString(clusterNameSansTenant) {
		return msg, "", roachpb.MaxTenantID, errors.Errorf("invalid cluster name '%s'", clusterNameFromDB)
	}

	tenID, err := strconv.ParseUint(tenantIDStr, 10, 64)
	if err != nil {
		return msg, "", roachpb.MaxTenantID, errors.Wrapf(err, "cannot parse %s as uint64", tenantIDStr)
	}

	// Make and return a copy of the startup msg so the original is not modified.
	paramsOut := map[string]string{}
	for key, value := range msg.Parameters {
		if key == "database" {
			paramsOut[key] = databaseName
		} else if key != "options" {
			paramsOut[key] = value
		}
	}
	outMsg := &pgproto3.StartupMessage{
		ProtocolVersion: msg.ProtocolVersion,
		Parameters:      paramsOut,
	}

	return outMsg, clusterNameSansTenant, roachpb.MakeTenantID(tenID), nil
}

// parseDatabaseParam parses the database parameter from the PG connection
// string, and tries to extract the cluster name if present. The cluster
// name should be embedded in the database parameter using the dot (".")
// delimiter in the form of "<cluster name>.<database name>". This approach
// is safe because dots are not allowed in the database names themselves.
func parseDatabaseParam(databaseParam string) (clusterName, databaseName string, err error) {
	// Database param is not provided.
	if databaseParam == "" {
		return "", "", nil
	}

	parts := strings.SplitN(databaseParam, ".", 2)

	// Database param provided without cluster name.
	if len(parts) <= 1 {
		return "", databaseParam, nil
	}

	clusterName, databaseName = parts[0], parts[1]

	// Ensure that the param is in the right format if the delimiter is provided.
	if len(parts) > 2 || clusterName == "" || databaseName == "" {
		return "", "", errors.New("invalid database param")
	}

	return clusterName, databaseName, nil
}

// parseOptionsParam parses the options parameter from the PG connection string,
// and tries to return the cluster name if present. Just like PostgreSQL, the
// sqlproxy supports three different ways to set a run-time parameter through
// its command-line options:
//     -c NAME=VALUE (commonly used throughout documentation around PGOPTIONS)
//     -cNAME=VALUE
//     --NAME=VALUE
//
// CockroachDB currently does not support the options parameter, so the parsing
// logic is built on that assumption. If we do start supporting options in
// CockroachDB itself, then we should revisit this.
//
// Note that this parsing approach is not perfect as it allows a negative case
// like options="-c --cluster=happy-koala -c -c -c" to go through. To properly
// parse this, we need to traverse the string from left to right, and look at
// every single argument, but that involves quite a bit of work, so we'll punt
// for now.
func parseOptionsParam(optionsParam string) (string, error) {
	// Only search up to 2 in case of large inputs.
	matches := clusterNameLongOptionRE.FindAllStringSubmatch(optionsParam, 2 /* n */)
	if len(matches) == 0 {
		return "", nil
	}

	if len(matches) > 1 {
		// Technically we could still allow requests to go through if all
		// cluster names match, but we don't want to parse the entire string, so
		// we will just error out if at least two cluster flags are provided.
		return "", errors.New("multiple cluster flags provided")
	}

	// Length of each match should always be 2 with the given regex, one for
	// the full string, and the other for the cluster name.
	if len(matches[0]) != 2 {
		// We don't want to panic here.
		return "", errors.New("internal server error")
	}

	// Flag was provided, but value is NULL.
	if len(matches[0][1]) == 0 {
		return "", errors.New("invalid cluster flag")
	}

	return matches[0][1], nil
}

// incomingTLSConfig gets back the current TLS config for the incoiming client
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
func (handler *proxyHandler) setupIncomingCert() error {
	if (handler.ListenKey == "") != (handler.ListenCert == "") {
		return errors.New("must specify either both or neither of cert and key")
	}

	if handler.ListenCert == "" {
		return nil
	}

	// TODO(darin): change the cert manager so it uses the stopper.
	ctx, _ := handler.stopper.WithCancelOnQuiesce(context.Background())
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
