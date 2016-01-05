// Copyright 2016 The Cockroach Authors.
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
// Author: Matt Jibson (mjibson@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
)

// TestRuby connects to a cluster with ruby.
func TestRuby(t *testing.T) {
	c := StartCluster(t)
	defer c.AssertAndStop(t)

	l, ok := c.(*cluster.LocalCluster)
	if !ok {
		return
	}

	addr := l.Nodes[0].PGAddr()

	cmd := exec.Command("docker",
		"run",
		"--rm",
		"--net=host",
		"-e", fmt.Sprintf("PGHOST=%s", addr.IP),
		"-e", fmt.Sprintf("PGPORT=%d", addr.Port),
		"-v", fmt.Sprintf("%s:%s", l.CertsDir, "/certs"),
		"-e", "PGSSLCERT=/certs/node.client.crt",
		"-e", "PGSSLKEY=/certs/node.client.key",
		"cockroachdb/postgres-test:ruby",
		"bash")
	cmd.Stdin = strings.NewReader(ruby)
	var buf bytes.Buffer
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Log(buf.String())
		t.Fatal(err)
	}
	if !cmd.ProcessState.Success() {
		t.Fatal("failed")
	}
}

// TODO(mjibson): stop using bash for its return value
// Invoking ruby directly in docker, I can't figure out how to get its
// exit status.
const ruby = `
ruby << 'EOF'
require 'pg'

conn = PG.connect(host: ENV['PGHOST'], port: ENV['PGPORT'])
res = conn.exec_params('SELECT 1, 2 > $1, $1', [3])
raise 'Unexpected: ' + res.values.to_s unless res.values == [["1", "f", "3"]]
EOF
`
