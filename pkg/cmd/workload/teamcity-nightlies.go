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
	"os/user"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var teamcityNightliesCmd = &cobra.Command{
	Use:  `teamcity-nightlies <build>`,
	Args: cobra.ExactArgs(1),
	RunE: runTeamcityNightlies,
}

func init() {
	rootCmd.AddCommand(teamcityNightliesCmd)
}

var singleDCTests = map[string]string{
	// `kv0`: `./kv --read-percent=0 --splits=1000 --concurrency=384 --duration=10m`,
	// `kv95`: `./kv --read-percent=95 --splits=1000 --concurrency=384 --duration=10m`,
	`kv95`: `./workload run kv --read-percent=95 --concurrency=384 --duration=10s`,
}

func runTeamcityNightlies(_ *cobra.Command, args []string) error {
	build := args[0]
	u, err := user.Current()
	if err != nil {
		return err
	}
	for name, singleDCTest := range singleDCTests {
		clusterName := fmt.Sprintf(`teamcity-nightly-workload-%s-%s`, build, name)
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
			exec.Command(`ssh-add`, filepath.Join(u.HomeDir, `.ssh/google_compute_engine`)),
			exec.Command(`roachperf`, clusterName, `put`, `./cockroach`, `./cockroach`),
			exec.Command(`roachperf`, clusterName, `put`, `./workload`, `./workload`),
			exec.Command(`roachperf`, clusterName, `workload-test`, name, singleDCTest),
		}
		for _, cmd := range cmds {
			fmt.Printf("> %s\n", strings.Join(cmd.Args, ` `))
			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Errorf("%s\n\n%s\n\n%s", strings.Join(cmd.Args, ` `), err, string(output))
			}
		}
	}
	return nil
}
