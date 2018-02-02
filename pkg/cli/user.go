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
	RunE: MaybeDecorateGRPCError(runGetUser),
}

func runGetUser(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return usageAndError(cmd)
	}
	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()
	return runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`SELECT * FROM system.public.users WHERE username=$1 AND "isRole" = false`, args[0]))
}

// A lsUsersCmd command displays a list of users.
var lsUsersCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all users",
	Long: `
List all users.
`,
	RunE: MaybeDecorateGRPCError(runLsUsers),
}

func runLsUsers(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return usageAndError(cmd)
	}
	conn, err := getPasswordAndMakeSQLClient()
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
	RunE: MaybeDecorateGRPCError(runRmUser),
}

func runRmUser(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return usageAndError(cmd)
	}
	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()
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
	RunE: MaybeDecorateGRPCError(runSetUser),
}

// runSetUser prompts for a password, then inserts the user and hash
// into the system.users table.
// TODO(marc): once we have more fields in the user, we will need
// to allow changing just some of them (eg: change email, but leave password).
func runSetUser(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return usageAndError(cmd)
	}
	pwdString := ""
	if password {
		var err error
		pwdString, err = security.PromptForPasswordTwice()
		if err != nil {
			return err
		}
	}

	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`CREATE USER IF NOT EXISTS $1`, args[0])); err != nil {
		return err
	}
	if password {
		// TODO(asubiotto): Implement appropriate server-side authorization rules
		// for users to be able to change their own passwords.
		return runQueryAndFormatResults(conn, os.Stdout,
			makeQuery(`ALTER USER $1 WITH PASSWORD $2`, args[0], pwdString))
	}
	return nil
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
