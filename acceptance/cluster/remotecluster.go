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
// Author: Tobias Schottdorf

package cluster

import (
	"flag"
	"fmt"
	"os/user"
	"path/filepath"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

func defaultKeyFile() string {
	base := "."
	me, err := user.Current()
	if err == nil {
		base = me.HomeDir
	}
	return filepath.Join(base, ".ssh/cockroach.pem")
}

var keyFile = flag.String("k", defaultKeyFile(), "SSH private/public key pair")
var login = flag.String("u", "ubuntu", "SSH user name")

type node struct {
	ID   string
	Name string
	Addr string
}

// RemoteCluster manages a remote cluster specified by a number of hosts.
type RemoteCluster struct {
	nodes      []node
	user, cert string
}

// CreateRemote connects to a remote cockroach cluster (which is required to
// have been set up separately).
func CreateRemote(peers []string) *RemoteCluster {
	nodes := make([]node, len(peers))
	for i := range nodes {
		nodes[i].Addr = peers[i]
		nodes[i].ID = fmt.Sprintf("roach%d", i)
		nodes[i].Name = nodes[i].ID
	}
	return &RemoteCluster{
		user:  *login,
		cert:  *keyFile,
		nodes: nodes,
	}
}

// Exec executes the given command on the i-th node, returning (in that order)
// stdout, stderr and an error.
func (r *RemoteCluster) Exec(i int, cmd string) (string, string, error) {
	return execute(r.user, r.nodes[i].Addr+":22", r.cert, cmd)
}

// NumNodes returns the number of nodes in the cluster, running or not.
func (r *RemoteCluster) NumNodes() int { return len(r.nodes) }

// MakeClient returns a client which is pointing at the node with the given
// index. The given integer must be in the range [0,NumNodes()-1].
func (r *RemoteCluster) MakeClient(t util.Tester, i int) (*client.DB, *stop.Stopper) {
	stopper := stop.NewStopper()

	// TODO(tschottdorf,mberhault): TLS all the things!
	db, err := client.Open(stopper, "rpc://"+"root"+"@"+
		util.EnsureHostPort(r.nodes[i].Addr)+
		"?certs="+"certswhocares")

	if err != nil {
		t.Fatal(err)
	}

	return db, stopper
}

// Assert verifies that the cluster state is as expected (i.e. no unexpected
// restarts or node deaths occurred). Tests can call this periodically to
// ascertain cluster health.
func (r *RemoteCluster) Assert(t util.Tester) {}

// AssertAndStop performs the same test as Assert but then proceeds to
// dismantle the cluster.
func (r *RemoteCluster) AssertAndStop(t util.Tester) {}

func (r *RemoteCluster) execLauncher(i int, action string, background bool) error {
	cmd := "./launch.sh " + action
	if background {
		cmd = "nohup " + cmd + " &> logs/nohup.out < /dev/null &"
	}
	o, e, err := r.Exec(i, cmd)
	if err != nil {
		log.Warningf("%s failed: %s\nstdout:\n%s\nstderr:\n%s", cmd, err, o, e)
	}
	return err
}

// Kill terminates the cockroach process running on the given node number.
// The given integer must be in the range [0,NumNodes()-1].
func (r *RemoteCluster) Kill(i int) error {
	return r.execLauncher(i, "kill", false /* !background */)
}

// Restart terminates the cockroach process running on the given node
// number, unless it is already stopped, and restarts it.
// The given integer must be in the range [0,NumNodes()-1].
func (r *RemoteCluster) Restart(i int) error {
	return r.execLauncher(i, "restart", true /* background */)
}

// Get queries the given node's HTTP endpoint for the given path, unmarshaling
// into the supplied interface (or returning an error).
func (r *RemoteCluster) Get(i int, path string, v interface{}) error {
	return getJSON(false /* !tls */, util.EnsureHostPort(r.nodes[i].Addr), path, v)
}
