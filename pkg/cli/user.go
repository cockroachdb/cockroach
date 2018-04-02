// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cli

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/security"
)

var password bool

// A getUserCmd command displays the config for the specified username.
var getUserCmd = &cobra.Command{
	Use:   "get [options] <username>",
	Short: "fetches and displays a user",
	Long: `
Fetches and displays the user for <username>.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runGetUser),
}

func runGetUser(cmd *cobra.Command, args []string) error {
	conn, err := getPasswordAndMakeSQLClient("cockroach user")
	if err != nil {
		return err
	}
	defer conn.Close()
	// NOTE: We too aggressively broke backwards compatibility in this command.
	// Future changes should maintain compatibility with the last two released
	// versions of CockroachDB.
	if err := conn.requireServerVersion(">=v2.0-alpha.20180116"); err != nil {
		return err
	}
	return runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`SELECT * FROM system.users WHERE username=$1 AND "isRole" = false`, args[0]))
}

// A lsUsersCmd command displays a list of users.
var lsUsersCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all users",
	Long: `
List all users.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runLsUsers),
}

func runLsUsers(cmd *cobra.Command, args []string) error {
	conn, err := getPasswordAndMakeSQLClient("cockroach user")
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
	Short: "remove a user",
	Long: `
Remove an existing user by username.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runRmUser),
}

func runRmUser(cmd *cobra.Command, args []string) error {
	conn, err := getPasswordAndMakeSQLClient("cockroach user")
	if err != nil {
		return err
	}
	defer conn.Close()
	// NOTE: We too aggressively broke backwards compatibility in this command.
	// Future changes should maintain compatibility with the last two released
	// versions of CockroachDB.
	if err := conn.requireServerVersion(">=v1.1-alpha.20170622"); err != nil {
		return err
	}
	return runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`DROP USER $1`, args[0]))
}

// A setUserCmd command creates a new or updates an existing user.
var setUserCmd = &cobra.Command{
	Use:   "set [options] <username>",
	Short: "create or update a user",
	Long: `
Create or update a user for the specified username, prompting
for the password.

Valid usernames contain 1 to 63 alphanumeric characters. They must
begin with either a letter or an underscore. Subsequent characters
may be letters, numbers, or underscores.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runSetUser),
}

// runSetUser prompts for a password, then inserts the user and hash
// into the system.users table.
// TODO(marc): once we have more fields in the user, we will need
// to allow changing just some of them (eg: change email, but leave password).
func runSetUser(cmd *cobra.Command, args []string) error {
	pwdString := ""
	if password {
		var err error
		pwdString, err = security.PromptForPasswordTwice()
		if err != nil {
			return err
		}
	}

	conn, err := getPasswordAndMakeSQLClient("cockroach user")
	if err != nil {
		return err
	}
	defer conn.Close()

	// NOTE: We too aggressively broke backwards compatibility in this command.
	// Future changes should maintain compatibility with the last two released
	// versions of CockroachDB.
	if err := conn.requireServerVersion(">=v1.2-alpha.20171113"); err != nil {
		return err
	}

	if password {
		return runQueryAndFormatResults(conn, os.Stdout,
			makeQuery(`CREATE USER IF NOT EXISTS $1 PASSWORD $2`, args[0], pwdString),
		)
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
	Short: "get, set, list and remove users",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Usage()
	},
}

func init() {
	userCmd.AddCommand(userCmds...)
}
