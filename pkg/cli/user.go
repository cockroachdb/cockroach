// Copyright 2015 The Cockroach Authors.
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
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/lib/pq"
	"github.com/spf13/cobra"
)

// TODO(knz): Remove this file after 19.2 is released.
// Then also remove the flag definitions in flags.go.

var password bool

// A getUserCmd command displays the config for the specified username.
var getUserCmd = &cobra.Command{
	Use:   "get [options] <username>",
	Short: "fetches and displays a user (deprecated)",
	Long: `
This command is deprecated.
Use SHOW USERS or SHOW ROLES in a SQL session.`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runGetUser),
}

var verGetUser = version.MustParse("v2.0.0-alpha.20180116")
var verRmUser = version.MustParse("v1.1.0-alpha.20170622")
var verSetUser = version.MustParse("v1.2.0-alpha.20171113")

func runGetUser(cmd *cobra.Command, args []string) error {
	fmt.Fprintf(stderr, "warning: %s\n", strings.ReplaceAll(strings.TrimSpace(cmd.Long), "\n", " "))
	conn, err := makeSQLClient("cockroach user", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()
	// NOTE: We too aggressively broke backwards compatibility in this command.
	// Future changes should maintain compatibility with the last two released
	// versions of CockroachDB.
	if err := conn.requireServerVersion(verGetUser); err != nil {
		return err
	}
	return runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`
SELECT username AS user_name,
       "isRole" as is_role
  FROM system.users
 WHERE username = $1 AND "isRole" = false`, args[0]))
}

// A lsUsersCmd command displays a list of users.
var lsUsersCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all users (deprecated)",
	Long: `
This command is deprecated.
Use SHOW USERS or SHOW ROLES in a SQL session.`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runLsUsers),
}

func runLsUsers(cmd *cobra.Command, args []string) error {
	fmt.Fprintf(stderr, "warning: %s\n", strings.ReplaceAll(strings.TrimSpace(cmd.Long), "\n", " "))
	conn, err := makeSQLClient("cockroach user", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()
	return runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`SHOW USERS`))
}

// A rmUserCmd command removes the user for the specified username.
var rmUserCmd = &cobra.Command{
	Use:   "rm [options] <username>",
	Short: "remove a user (deprecated)",
	Long: `
This command is deprecated.
Use DROP USER or DROP ROLE in a SQL session.`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runRmUser),
}

func runRmUser(cmd *cobra.Command, args []string) error {
	fmt.Fprintf(stderr, "warning: %s\n", strings.ReplaceAll(strings.TrimSpace(cmd.Long), "\n", " "))
	conn, err := makeSQLClient("cockroach user", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()
	// NOTE: We too aggressively broke backwards compatibility in this command.
	// Future changes should maintain compatibility with the last two released
	// versions of CockroachDB.
	if err := conn.requireServerVersion(verRmUser); err != nil {
		return err
	}
	return runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`DROP USER $1`, args[0]))
}

// A setUserCmd command creates a new or updates an existing user.
var setUserCmd = &cobra.Command{
	Use:   "set [options] <username>",
	Short: "create or update a user (deprecated)",
	Long: `
This command is deprecated.
Use CREATE USER or ALTER USER ... WITH PASSWORD ... in a SQL session.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runSetUser),
}

// runSetUser prompts for a password, then inserts the user and hash
// into the system.users table.
// TODO(marc): once we have more fields in the user, we will need
// to allow changing just some of them (eg: change email, but leave password).
func runSetUser(cmd *cobra.Command, args []string) error {
	fmt.Fprintf(stderr, "warning: %s\n", strings.ReplaceAll(strings.TrimSpace(cmd.Long), "\n", " "))
	pwdString := ""
	if password {
		var err error
		pwdString, err = security.PromptForPasswordTwice()
		if err != nil {
			return err
		}
	}

	conn, err := makeSQLClient("cockroach user", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	// NOTE: We too aggressively broke backwards compatibility in this command.
	// Future changes should maintain compatibility with the last two released
	// versions of CockroachDB.
	if err := conn.requireServerVersion(verSetUser); err != nil {
		return err
	}

	if password {
		if err := runQueryAndFormatResults(conn, os.Stdout,
			makeQuery(`CREATE USER $1 PASSWORD $2`, args[0], pwdString),
		); err != nil {
			if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == pgcode.DuplicateObject {
				return runQueryAndFormatResults(conn, os.Stdout,
					makeQuery(`ALTER USER $1 WITH PASSWORD $2`, args[0], pwdString))
			}
			return err
		}
	}
	return runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`CREATE USER IF NOT EXISTS $1`, args[0]))
}

var userCmds = []*cobra.Command{
	getUserCmd,
	lsUsersCmd,
	rmUserCmd,
	setUserCmd,
}

var userCmd = &cobra.Command{
	Use:   "user",
	Short: "get, set, list and remove users (deprecated)",
	RunE:  usageAndErr,
}

func init() {
	userCmd.AddCommand(userCmds...)
}
