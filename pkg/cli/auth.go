// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"net/http"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	isatty "github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
)

var loginCmd = &cobra.Command{
	Use:   "login [options] <session-username>",
	Short: "create a HTTP session and token for the given user",
	Long: `
Creates a HTTP session for the given user and print out a login cookie for use
in non-interactive programs.

Example use of the session cookie using 'curl':

   curl -k -b "<cookie>" https://localhost:8080/_admin/v1/settings

The user invoking the 'login' CLI command must be an admin on the cluster.
The user for which the HTTP session is opened can be arbitrary.
`,
	Args: cobra.ExactArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runLogin),
}

func runLogin(cmd *cobra.Command, args []string) error {
	// In CockroachDB SQL, unlike in PostgreSQL, usernames are
	// case-insensitive. Therefore we need to normalize the username
	// here, so that the normalized username is retained in the session
	// table: the APIs extract the username from the session table
	// without further normalization.
	username := tree.Name(args[0]).Normalize()

	id, httpCookie, err := createAuthSessionToken(username)
	if err != nil {
		return err
	}
	hC := httpCookie.String()

	if authCtx.onlyCookie {
		// Simple format suitable for automation.
		fmt.Println(hC)
	} else {
		// More complete format, suitable e.g. for appending to a CSV file
		// with --format=csv.
		cols := []string{"username", "session ID", "authentication cookie"}
		rows := [][]string{
			{username, fmt.Sprintf("%d", id), hC},
		}
		if err := sqlExecCtx.PrintQueryOutput(os.Stdout, stderr, cols, clisqlexec.NewRowSliceIter(rows, "ll")); err != nil {
			return err
		}

		if isatty.IsTerminal(os.Stdin.Fd()) {
			fmt.Fprintf(stderr, `#
# Example uses:
#
#     curl [-k] --cookie '%[1]s' https://...
#
#     wget [--no-check-certificate] --header='Cookie: %[1]s' https://...
#
`, hC)
		}
	}

	return nil
}

func createAuthSessionToken(
	username string,
) (sessionID int64, httpCookie *http.Cookie, resErr error) {
	sqlConn, err := makeSQLClient("cockroach auth-session login", useSystemDb)
	if err != nil {
		return -1, nil, err
	}
	defer func() { resErr = errors.CombineErrors(resErr, sqlConn.Close()) }()

	ctx := context.Background()

	// First things first. Does the user exist?
	_, rows, err := sqlExecCtx.RunQuery(ctx,
		sqlConn,
		clisqlclient.MakeQuery(`SELECT count(username) FROM system.users WHERE username = $1 AND NOT "isRole"`, username), false)
	if err != nil {
		return -1, nil, err
	}
	if rows[0][0] != "1" {
		return -1, nil, fmt.Errorf("user %q does not exist", username)
	}

	// Make a secret.
	secret, hashedSecret, err := server.CreateAuthSecret()
	if err != nil {
		return -1, nil, err
	}
	expiration := timeutil.Now().Add(authCtx.validityPeriod)

	// Create the session on the server to the server.
	insertSessionStmt := `
INSERT INTO system.web_sessions ("hashedSecret", username, "expiresAt")
VALUES($1, $2, $3)
RETURNING id
`
	var id int64
	row, err := sqlConn.QueryRow(ctx,
		insertSessionStmt,
		hashedSecret,
		username,
		expiration,
	)
	if err != nil {
		return -1, nil, err
	}
	if len(row) != 1 {
		return -1, nil, errors.Newf("expected 1 column, got %d", len(row))
	}
	id, ok := row[0].(int64)
	if !ok {
		return -1, nil, errors.Newf("expected integer, got %T", row[0])
	}

	// Spell out the cookie.
	sCookie := &serverpb.SessionCookie{ID: id, Secret: secret}
	httpCookie, err = server.EncodeSessionCookie(sCookie, false /* forHTTPSOnly */)
	return id, httpCookie, err
}

var logoutCmd = &cobra.Command{
	Use:   "logout [options] <session-username>",
	Short: "invalidates all the HTTP session tokens previously created for the given user",
	Long: `
Revokes all previously issued HTTP authentication tokens for the given user.

The user invoking the 'login' CLI command must be an admin on the cluster.
The user for which the HTTP sessions are revoked can be arbitrary.
`,
	Args: cobra.ExactArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runLogout),
}

func runLogout(cmd *cobra.Command, args []string) (resErr error) {
	username := tree.Name(args[0]).Normalize()

	sqlConn, err := makeSQLClient("cockroach auth-session logout", useSystemDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, sqlConn.Close()) }()

	logoutQuery := clisqlclient.MakeQuery(
		`UPDATE system.web_sessions SET "revokedAt" = if("revokedAt"::timestamptz<now(),"revokedAt",now())
      WHERE username = $1
  RETURNING username,
            id AS "session ID",
            "revokedAt" AS "revoked"`,
		username)
	return sqlExecCtx.RunQueryAndFormatResults(
		context.Background(),
		sqlConn, os.Stdout, stderr, logoutQuery)
}

var authListCmd = &cobra.Command{
	Use:   "list",
	Short: "lists the currently active HTTP sessions",
	Long: `
Prints out the currently active HTTP sessions.

The user invoking the 'list' CLI command must be an admin on the cluster.
`,
	Args: cobra.ExactArgs(0),
	RunE: clierrorplus.MaybeDecorateError(runAuthList),
}

func runAuthList(cmd *cobra.Command, args []string) (resErr error) {
	sqlConn, err := makeSQLClient("cockroach auth-session list", useSystemDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, sqlConn.Close()) }()

	logoutQuery := clisqlclient.MakeQuery(`
SELECT username,
       id AS "session ID",
       "createdAt" as "created",
       "expiresAt" as "expires",
       "revokedAt" as "revoked",
       "lastUsedAt" as "last used"
  FROM system.web_sessions`)
	return sqlExecCtx.RunQueryAndFormatResults(
		context.Background(),
		sqlConn, os.Stdout, stderr, logoutQuery)
}

var authCmds = []*cobra.Command{
	loginCmd,
	logoutCmd,
	authListCmd,
}

var authCmd = &cobra.Command{
	Use:   "auth-session",
	Short: "log in and out of HTTP sessions",
	RunE:  UsageAndErr,
}

func init() {
	authCmd.AddCommand(authCmds...)
}
