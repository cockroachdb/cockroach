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
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

The user invoking the 'login' CLI command must be an admin on the cluster;
the user for which the HTTP session is opened can be arbitrary.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runLogin),
}

func runLogin(cmd *cobra.Command, args []string) error {
	sqlConn, err := makeSQLClient("cockroach auth-session login", useSystemDb)
	if err != nil {
		return err
	}
	defer sqlConn.Close()

	username := args[0]

	secret, hashedSecret, err := server.CreateAuthSecret()
	if err != nil {
		return err
	}

	expiration := timeutil.Now().Add(1 * time.Hour)

	insertSessionStmt := `
INSERT INTO system.web_sessions ("hashedSecret", username, "expiresAt")
VALUES($1, $2, $3)
RETURNING id
`
	var id int64
	row, err := sqlConn.QueryRow(
		insertSessionStmt,
		[]driver.Value{
			hashedSecret,
			username,
			expiration,
		},
	)
	if err != nil {
		return err
	}
	if len(row) != 1 {
		return errors.Newf("expected 1 column, got %d", len(row))
	}
	id, ok := row[0].(int64)
	if !ok {
		return errors.Newf("expected integer, got %T", row[0])
	}

	fmt.Println("Session ID:", id)

	sCookie := &serverpb.SessionCookie{ID: id, Secret: secret}
	httpCookie, err := server.EncodeSessionCookie(sCookie)
	if err != nil {
		return err
	}

	fmt.Println("Authentication cookie:", httpCookie.String())

	return nil
}

var logoutCmd = &cobra.Command{
	Use:   "logout [options] <session-username>",
	Short: "invalidates all the HTTP session tokens previously created for the given user",
	Long: `
Revokes all previously issued HTTP authentication tokens for the given user.

The user invoking the 'login' CLI command must be an admin on the cluster;
the user for which the HTTP sessions are revoked can be arbitrary.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runLogout),
}

func runLogout(cmd *cobra.Command, args []string) error {
	sqlConn, err := makeSQLClient("cockroach auth-session logout", useSystemDb)
	if err != nil {
		return err
	}
	defer sqlConn.Close()

	username := args[0]

	err = sqlConn.Exec(
		`UPDATE system.web_sessions SET "revokedAt" = now() WHERE username = $1`,
		[]driver.Value{username})
	return err
}

var authCmds = []*cobra.Command{
	loginCmd,
	logoutCmd,
}

var authCmd = &cobra.Command{
	Use:   "auth-session",
	Short: "log in and out of HTTP sessions",
	RunE:  usageAndErr,
}

func init() {
	authCmd.AddCommand(authCmds...)
}
