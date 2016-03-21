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
	"bufio"
	"os"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/security"
)

var password string

// A getUserCmd command displays the config for the specified username.
var getUserCmd = &cobra.Command{
	Use:   "get [options] <username>",
	Short: "fetches and displays a user config",
	Long: `
Fetches and displays the user configuration for <username>.
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
	err = runPrettyQuery(conn, os.Stdout,
		makeQuery(`SELECT * FROM system.users WHERE username=$1`, args[0]))
	if err != nil {
		panic(err)
	}
}

// A lsUsersCmd command displays a list of user configs.
var lsUsersCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all user configs",
	Long: `
List all user configs.
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
	err = runPrettyQuery(conn, os.Stdout,
		makeQuery(`SELECT username FROM system.users`))
	if err != nil {
		panic(err)
	}
}

// A rmUserCmd command removes the user config for the specified username.
var rmUserCmd = &cobra.Command{
	Use:   "rm [options] <username>",
	Short: "remove a user config",
	Long: `
Remove an existing user config by username.
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
	err = runPrettyQuery(conn, os.Stdout,
		makeQuery(`DELETE FROM system.users WHERE username=$1`, args[0]))
	if err != nil {
		panic(err)
	}
}

// A setUserCmd command creates a new or updates an existing user config.
var setUserCmd = &cobra.Command{
	Use:   "set [options] <username>",
	Short: "create or update a user config for key prefix",
	Long: `
Create or update a user config for the specified username, prompting
for the password.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runSetUser),
}

// runSetUser prompts for a password, then inserts the user and hash
// into the system.users table.
// TODO(marc): once we have more fields in the user config, we will need
// to allow changing just some of them (eg: change email, but leave password).
func runSetUser(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		mustUsage(cmd)
		return
	}
	var err error
	var hashed []byte
	switch password {
	case "":
		hashed, err = security.PromptForPasswordAndHash()
		if err != nil {
			panic(err)
		}
	case "-":
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			if b := scanner.Bytes(); len(b) > 0 {
				hashed, err = security.HashPassword(b)
				if err != nil {
					panic(err)
				}
				if scanner.Scan() {
					panic("multiline passwords are not permitted")
				}
				if err := scanner.Err(); err != nil {
					panic(err)
				}

				break // Success.
			}
		} else {
			if err := scanner.Err(); err != nil {
				panic(err)
			}
		}

		panic("empty passwords are not permitted")
	default:
		hashed, err = security.HashPassword([]byte(password))
		if err != nil {
			panic(err)
		}
	}
	conn, err := makeSQLClient()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	// TODO(marc): switch to UPSERT.
	err = runPrettyQuery(conn, os.Stdout,
		makeQuery(`INSERT INTO system.users VALUES ($1, $2)`, args[0], hashed))
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
