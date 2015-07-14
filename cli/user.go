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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package cli

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util/log"

	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v1"
)

// A getUserCmd command displays the config for the specified username.
var getUserCmd = &cobra.Command{
	Use:   "get [options] <username>",
	Short: "fetches and displays a user config",
	Long: `
Fetches and displays the user configuration for <username>.
`,
	Run: runGetUser,
}

// runGetUser invokes the REST API with GET action and username as path.
func runGetUser(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	admin := client.NewAdminClient(&Context.Context, Context.Addr, client.User)
	body, err := admin.GetYAML(args[0])
	if err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("User config for %q:\n%s\n", args[0], body)
}

// A lsUsersCmd command displays a list of user configs.
var lsUsersCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all user configs",
	Long: `
List all user configs.
`,
	Run: runLsUsers,
}

// runLsUsers invokes the REST API with GET action and no path, which
// fetches a list of all user configuration.
func runLsUsers(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		cmd.Usage()
		return
	}
	admin := client.NewAdminClient(&Context.Context, Context.Addr, client.User)
	list, err := admin.List()
	if err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Users:\n%s\n", strings.Join(list, "\n  "))

}

// A rmUserCmd command removes the user config for the specified username.
var rmUserCmd = &cobra.Command{
	Use:   "rm [options] <username>",
	Short: "remove a user config",
	Long: `
Remove an existing user config by username.
`,
	Run: runRmUser,
}

// runRmUser invokes the REST API with DELETE action and username as path.
func runRmUser(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	admin := client.NewAdminClient(&Context.Context, Context.Addr, client.User)
	if err := admin.Delete(args[0]); err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Deleted user %q\n", args[0])

}

// A setUserCmd command creates a new or updates an existing user config.
var setUserCmd = &cobra.Command{
	Use:   "set [options] <username>",
	Short: "create or update a user config for key prefix",
	Long: `
Create or update a user config for the specified username, prompting
for the password.
`,
	Run: runSetUser,
}

// runSetUser invokes the REST API with POST action and username as
// path. Prompts for the password twice on stdin.
// TODO(marc): once we have more fields in the user config, we will need
// to allow changing just some of them (eg: change email, but leave password).
func runSetUser(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	hashed, err := security.PromptForPasswordAndHash()
	if err != nil {
		log.Error(err)
		return
	}
	// Build a UserConfig object. RunSetUser expects Yaml.
	// TODO(marc): re-work admin client library to take other encodings.
	pb := &proto.UserConfig{HashedPassword: hashed}
	contents, err := yaml.Marshal(pb)
	if err != nil {
		log.Error(err)
		return
	}
	admin := client.NewAdminClient(&Context.Context, Context.Addr, client.User)
	if err := admin.SetYAML(args[0], string(contents)); err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Wrote user config for %q\n", args[0])
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
		cmd.Usage()
	},
}

func init() {
	userCmd.AddCommand(userCmds...)
}
