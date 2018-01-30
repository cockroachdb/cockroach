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

// +build nightly

package nightly

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func makeClusterName(id string) string {
	buildID := os.Getenv("TC_BUILD_ID")
	return fmt.Sprintf("teamcity-nightly-%s-%s", buildID, id)
}

func TestKV(t *testing.T) {
	t.Parallel()

	for _, p := range []int{95, 0} {
		p := p
		t.Run(fmt.Sprintf("read-percent=%d", p), func(t *testing.T) {
			t.Parallel()
			clusterName := makeClusterName(fmt.Sprintf("kv_%d", p))

			cmds := []*exec.Cmd{
				exec.Command(`roachprod`, `-u`, `teamcity`, `create`, clusterName),
				exec.Command(`roachprod`, `sync`),
				exec.Command(`roachprod`, clusterName, `put`, `./cockroach`, `./cockroach`),
				exec.Command(`roachprod`, clusterName, `put`, `./workload`, `./workload`),
				exec.Command(`roachprod`, clusterName, `workload-test`, fmt.Sprintf(`kv%d`, p),
					fmt.Sprintf(`./workload run kv --read-percent=%d --concurrency=384 --duration=10m`, p)),
			}
			for _, cmd := range cmds {
				fmt.Printf("> %s\n", strings.Join(cmd.Args, ` `))
				// output, err := cmd.CombinedOutput()
				// if err != nil {
				// 	return errors.Errorf("%s\n\n%s\n\n%s", strings.Join(cmd.Args, ` `), err, string(output))
				// }
			}
		})
	}
}
