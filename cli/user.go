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
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package cli

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/security"
)

var password string

// A getUserCmd command displays the config for the specified username.
var getUserCmd = &cobra.Command{
	Use:   "get [options] <username>",
	Short: "fetches and displays a user",
	Long: `
Fetches and displays the user for <username>.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runGetUser),
}

func runGetUser(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		mustUsage(cmd)
		return
	}
	conn, err := makeSQLClient()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	err = runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`SELECT * FROM system.users WHERE username=$1`, args[0]), cliCtx.prettyFmt)
	if err != nil {
		panic(err)
	}
}

// A lsUsersCmd command displays a list of users.
var lsUsersCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all users",
	Long: `
List all users.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runLsUsers),
}

func runLsUsers(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		mustUsage(cmd)
		return
	}
	conn, err := makeSQLClient()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	err = runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`SELECT username FROM system.users`), cliCtx.prettyFmt)
	if err != nil {
		panic(err)
	}
}

// A rmUserCmd command removes the user for the specified username.
var rmUserCmd = &cobra.Command{
	Use:   "rm [options] <username>",
	Short: "remove a user",
	Long: `
Remove an existing user by username.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runRmUser),
}

func runRmUser(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		mustUsage(cmd)
		return
	}
	conn, err := makeSQLClient()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	err = runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`DELETE FROM system.users WHERE username=$1`, args[0]), cliCtx.prettyFmt)
	if err != nil {
		panic(err)
	}
}

// A setUserCmd command creates a new or updates an existing user.
var setUserCmd = &cobra.Command{
	Use:   "set [options] <username>",
	Short: "create or update a user",
	Long: `
Create or update a user for the specified username, prompting
for the password.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runSetUser),
}

// runSetUser prompts for a password, then inserts the user and hash
// into the system.users table.
// TODO(marc): once we have more fields in the user, we will need
// to allow changing just some of them (eg: change email, but leave password).
func runSetUser(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		mustUsage(cmd)
		return
	}
	username := args[0]

	var err error
	var hashed string
	if password == "" {
		hashed, err = security.PromptForPasswordAndHash(username)
		if err != nil {
			panic(err)
		}
	} else {
		hashed = security.MD5Hash(password + username)
	}

	// TODO(asubiotto): Only have privilege to change password for a certain
	// user if we're connecting as security.RootUser. We might want to change
	// this once we have row security policies.
	conn, err := makeSQLClient()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	err = runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(`UPSERT INTO system.users VALUES ($1, $2)`, username, hashed), cliCtx.prettyFmt)
	if err != nil {
		panic(err)
	}
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
	Run: func(cmd *cobra.Command, args []string) {
		mustUsage(cmd)
	},
}

func init() {
	userCmd.AddCommand(userCmds...)
}
