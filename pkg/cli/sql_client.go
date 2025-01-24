// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"net/url"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgconn"
)

// sqlConnTimeout is the default SQL connect timeout. This can also be
// set using `connect_timeout` in the connection URL. The default of
// 15 seconds is chosen to exceed the default password retrieval
// timeout (system.user_login.timeout).
var sqlConnTimeout = envutil.EnvOrDefaultString("COCKROACH_CONNECT_TIMEOUT", "15")

// defaultSQLDb describes how a missing database part in the SQL
// connection string is processed when creating a client connection.
type defaultSQLDb int

const (
	// useSystemDb means that a missing database will be overridden with
	// "system".
	useSystemDb defaultSQLDb = iota
	// useDefaultDb means that a missing database will be left as-is so
	// that the server can default to "defaultdb".
	useDefaultDb
)

const userDefaultTenant = ""

// makeSQLClient calls makeTenantSQLClient with the user default tenant as the default.
func makeSQLClient(
	ctx context.Context, appName string, defaultMode defaultSQLDb,
) (clisqlclient.Conn, error) {
	return makeTenantSQLClient(ctx, appName, defaultMode, userDefaultTenant)
}

// makeTenantSQLClient runs makeTenantSQLClient using the default cliContext
// that has the connection settings set by the command-line flags.
func makeTenantSQLClient(
	ctx context.Context, appName string, defaultMode defaultSQLDb, tenantName string,
) (clisqlclient.Conn, error) {
	return cliCtx.makeTenantSQLClient(ctx, appName, defaultMode, tenantName)
}

// makeTenantSQLClient connects to the database using the connection settings in
// the cliContext. If a password is needed, it also prompts for the password.
//
// The appName given as argument is added to the URL even if --url is
// specified, but only if the URL didn't already specify
// application_name. It is prefixed with '$ ' to mark it as internal.
//
// If defaultMode is useSystemDB, it connects it to the `system` database,
// ignoring the --database flag or database part in the URL.
//
// Connections are made to the given tenantName using the ccluster option,
// unless userDefaultTenant is used. If the cluster returns an error indicating
// that the ccluster option is not supported, we retry without the ccluster
// option for compatibility with separate process SQL servers.
func (c *cliContext) makeTenantSQLClient(
	ctx context.Context, appName string, defaultMode defaultSQLDb, tenantName string,
) (clisqlclient.Conn, error) {
	baseURL, err := c.makeClientConnURL()
	if err != nil {
		return nil, err
	}
	cclusterOptionRequired := tenantName != userDefaultTenant
	if cclusterOptionRequired {
		// makeClientConnURL returns a memoized URL. If we've been asked
		// to connect to a particular tenant, clone the URL so that we
		// aren't constantly appending options to the same URL.
		baseURL = baseURL.Clone()
		err = baseURL.AddOptions(url.Values{
			"options": []string{"-ccluster=" + tenantName},
		})
		if err != nil {
			return nil, err
		}
	}

	conn, err := makeSQLClientForBaseURL(appName, defaultMode, baseURL)
	if err != nil && shouldTryWithoutTenantName(tenantName, err) {
		return c.makeTenantSQLClient(ctx, appName, defaultMode, userDefaultTenant)
	} else if err != nil {
		return nil, err
	}

	if cclusterOptionRequired {
		// If we've specified the ccluster option, we don't know if it
		// is supported until we actually try to make a connection.
		if err := conn.EnsureConn(ctx); err != nil && shouldTryWithoutTenantName(tenantName, err) {
			if err := conn.Close(); err != nil {
				log.VInfof(ctx, 2, "close err: %v", err)
			}
			return c.makeTenantSQLClient(ctx, appName, defaultMode, userDefaultTenant)
		}
	}

	return conn, nil
}

func makeSQLClientForBaseURL(
	appName string, defaultMode defaultSQLDb, baseURL *pgurl.URL,
) (clisqlclient.Conn, error) {
	// Set a connection timeout if none is provided already.
	var err error
	sqlCtx.ConnectTimeout, err = strconv.Atoi(sqlConnTimeout)
	if err != nil {
		return nil, err
	}

	if defaultMode == useSystemDb {
		// Override the target database. This is because the current
		// database can influence the output of CLI commands, and in the
		// case where the database is missing it will default server-wise to
		// `defaultdb` which may not exist.
		sqlCtx.Database = "system"
	}

	// If there is no user in the URL already, fill in the default user.
	sqlCtx.User = username.RootUser

	// If there is no application name already, use the provided one.
	sqlCtx.ApplicationName = catconstants.ReportableAppNamePrefix + appName

	// How we're going to authenticate.
	usePw, _, _ := baseURL.GetAuthnPassword()
	if usePw && cliCtx.Insecure {
		// There's a password already configured.
		// In insecure mode, we don't want the user to get the mistaken
		// idea that a password is worth anything.
		return nil, errors.Errorf("password authentication not enabled in insecure mode")
	}

	sqlURL := baseURL.ToPQ().String()

	if log.V(2) {
		log.Infof(context.Background(), "connecting with URL: %s", sqlURL)
	}

	return sqlCtx.MakeConn(sqlURL)
}

// shouldTryWithoutTenantName returns true if given error is from a connection
// attempt that should be retried without the ccluster option.
//
// Currently, we only expect to hit this when we are trying to explicitly use
// the system tenant but are talking to a standalone tenant process.
func shouldTryWithoutTenantName(tenantName string, err error) bool {
	// We only want to retry if the SystemTenant was requested.
	if tenantName != catconstants.SystemTenantName {
		return false
	}
	return maybeTenantSelectionNotSupportedErr(err)
}

// maybeSelectionNotSupportedErr returns true if the error may be the result of
// specifying the ccluster option when that option is not supported.
func maybeTenantSelectionNotSupportedErr(err error) bool {
	if strings.Contains(err.Error(), "tenant selection is not available on this server") {
		return true
	}
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
		return pgErr.Code == pgcode.InvalidParameterValue.String()
	}
	return pgerror.GetPGCode(err) == pgcode.InvalidParameterValue
}
