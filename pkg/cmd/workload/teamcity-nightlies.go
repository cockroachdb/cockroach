// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var teamcityNightliesCmd = &cobra.Command{
	Use:   `teamcity-nightlies <cluster-prefix>`,
	Short: `Run the nightly performance tests`,
	Args:  cobra.ExactArgs(1),
	RunE:  runTeamcityNightlies,
}

func init() {
	rootCmd.AddCommand(teamcityNightliesCmd)
}

// TODO(dan): It's not clear to me yet how all the various configurations of
// clusters (# of machines in each locality) are going to generalize. For now,
// just hardcode what we had before.
var singleDCTests = map[string]string{
	`kv0`:    `./workload run kv --init --read-percent=0 --splits=1000 --concurrency=384 --duration=10m`,
	`kv95`:   `./workload run kv --init --read-percent=95 --splits=1000 --concurrency=384 --duration=10m`,
	`splits`: `./workload run kv --init --read-percent=0 --splits=100000 --concurrency=384 --max-ops=1`,
}

func runTeamcityNightlies(_ *cobra.Command, args []string) error {
	clusterPrefix := args[0]
	for testName, testCmd := range singleDCTests {
		clusterName := fmt.Sprintf(`%s-%s`, clusterPrefix, testName)
		defer func(clusterName string) {
			cmd := exec.Command(`roachprod`, `-u`, `teamcity`, `destroy`, clusterName)
			fmt.Printf("> %s\n", strings.Join(cmd.Args, ` `))
			if output, err := cmd.CombinedOutput(); err != nil {
				fmt.Printf(`Failed to destoy %s\n%s\n\n%s\n\n%s`,
					clusterName, strings.Join(cmd.Args, ` `), err, string(output))
			}
		}(clusterName)
		cmds := []*exec.Cmd{
			exec.Command(`roachprod`, `-u`, `teamcity`, `create`, clusterName),
			exec.Command(`roachprod`, `sync`),
			exec.Command(`sleep`, `30`),
			exec.Command(`roachprod`, `put`, clusterName, `./cockroach`, `./cockroach`),
			exec.Command(`roachprod`, `put`, clusterName, `./workload`, `./workload`),
			exec.Command(`roachprod`, `workload-test`, clusterName, testName, testCmd),
		}
		for _, cmd := range cmds {
			fmt.Printf("> %s\n", strings.Join(cmd.Args, ` `))
			output, err := cmd.CombinedOutput()
			if err != nil {
				err := errors.Errorf("%s\n\n%s\n\n%s", strings.Join(cmd.Args, ` `), err, string(output))
				fmt.Println(err)
				return err
			}
		}
	}
	return nil
}
