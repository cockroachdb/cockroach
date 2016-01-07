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
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/samalba/dockerclient"
)

// TestRuby connects to a cluster with ruby.
func TestRuby(t *testing.T) {
	t.Skip("skip until docker issues fixed")

	l := MustStartLocal(t)
	defer l.AssertAndStop(t)

	addr := l.Nodes[0].PGAddr()

	client := cluster.NewDockerClient()
	containerConfig := dockerclient.ContainerConfig{
		Image: "cockroachdb/postgres-test:ruby",
		Env: []string{
			fmt.Sprintf("PGHOST=%s", addr.IP),
			fmt.Sprintf("PGPORT=%d", addr.Port),
			"PGSSLCERT=/certs/node.client.crt",
			"PGSSLKEY=/certs/node.client.key",
		},
		Cmd: []string{"ruby", "-e", ruby},
	}
	if err := client.PullImage(containerConfig.Image, nil); err != nil {
		t.Fatal(err)
	}
	id, err := client.CreateContainer(&containerConfig, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.RemoveContainer(id, false, false)

	hostConfig := dockerclient.HostConfig{
		Binds:       []string{fmt.Sprintf("%s:%s", l.CertsDir, "/certs")},
		NetworkMode: "host",
	}
	if err := client.StartContainer(id, &hostConfig); err != nil {
		t.Fatal(err)
	}
	wr := <-client.Wait(id)
	if wr.Error != nil {
		t.Fatal(wr.Error)
	}
	if wr.ExitCode != 0 {
		rc, err := client.ContainerLogs(id, &dockerclient.LogOptions{
			Stdout: true,
			Stderr: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		b, err := ioutil.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Log(err)
		} else {
			t.Log(string(b))
		}
		t.Fatalf("exit code: %d", wr.ExitCode)
	}
}

const ruby = `
require 'pg'

conn = PG.connect(host: ENV['PGHOST'], port: ENV['PGPORT'])
res = conn.exec_params('SELECT 1, 2 > $1, $1', [3])
raise 'Unexpected: ' + res.values.to_s unless res.values == [["1", "f", "3"]]
`
