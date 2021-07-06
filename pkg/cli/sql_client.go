// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

// makeSQLClient connects to the database using the connection
// settings set by the command-line flags.
// If a password is needed, it also prompts for the password.
//
// If forceSystemDB is set, it also connects it to the `system`
// database. The --database flag or database part in the URL is then
// ignored.
//
// The appName given as argument is added to the URL even if --url is
// specified, but only if the URL didn't already specify
// application_name. It is prefixed with '$ ' to mark it as internal.
func makeSQLClient(appName string, defaultMode defaultSQLDb) (clisqlclient.Conn, error) {
	baseURL, err := cliCtx.makeClientConnURL()
	if err != nil {
		return nil, err
	}

	if defaultMode == useSystemDb {
		// Override the target database. This is because the current
		// database can influence the output of CLI commands, and in the
		// case where the database is missing it will default server-wise to
		// `defaultdb` which may not exist.
		baseURL.WithDefaultDatabase("system")
	}

	// If there is no user in the URL already, fill in the default user.
	baseURL.WithDefaultUsername(security.RootUser)

	// How we're going to authenticate.
	usePw, pwdSet, _ := baseURL.GetAuthnPassword()
	if usePw {
		// There's a password already configured.

		// In insecure mode, we don't want the user to get the mistaken
		// idea that a password is worth anything.
		if cliCtx.Insecure {
			return nil, errors.Errorf("password authentication not enabled in insecure mode")
		}
	}

	// Load the application name. It's not a command-line flag, so
	// anything already in the URL should take priority.
	if prevAppName := baseURL.GetOption("application_name"); prevAppName == "" && appName != "" {
		_ = baseURL.SetOption("application_name", catconstants.ReportableAppNamePrefix+appName)
	}

	// Set a connection timeout if none is provided already. This
	// ensures that if the server was not initialized or there is some
	// network issue, the client will not be left to hang forever.
	//
	// This is a lib/pq feature.
	if baseURL.GetOption("connect_timeout") == "" {
		_ = baseURL.SetOption("connect_timeout", sqlConnTimeout)
	}

	sqlURL := baseURL.ToPQ().String()

	if log.V(2) {
		log.Infof(context.Background(), "connecting with URL: %s", sqlURL)
	}

	conn := makeSQLConn(sqlURL)
	conn.SetMissingPassword(!usePw || !pwdSet)

	return conn, nil
}
