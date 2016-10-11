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
// Author: Tobias Schottdorf

package terrafarm

import (
	"os/user"
	"path/filepath"
)

const sshUser = "ubuntu"

func (f *Farmer) defaultKeyFile() string {
	base := "."
	me, err := user.Current()
	if err == nil {
		base = me.HomeDir
	}
	return filepath.Join(base, ".ssh/"+f.KeyName)
}

func (f *Farmer) ssh(host, keyfile, cmd string) (stdout string, stderr string, _ error) {
	return f.run("ssh",
		"-o", "ServerAliveInterval=5",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-q", "-i", keyfile, sshUser+"@"+host, cmd)
}

func (f *Farmer) scp(host, keyfile, src, dest string) error {
	_, _, err := f.run("scp", "-r",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-q", "-i", keyfile,
		sshUser+"@"+host+":"+src, dest)
	return err
}
