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

package terrafarm

import (
	"bytes"
	"io/ioutil"
	"net"
	"path/filepath"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

const sshUser = "ubuntu"

func (f *Farmer) defaultKeyFile() string {
	base := "."
	me, err := envutil.HomeDir()
	if err == nil {
		base = me
	}
	return filepath.Join(base, ".ssh/"+f.KeyName)
}

func (f *Farmer) getSSH(host, keyfile string) (*ssh.Client, error) {
	for i := range f.nodes {
		node := &f.nodes[i]
		if node.hostname == host {
			if c := node.ssh; c != nil {
				return c, nil
			}
			c, err := newSSH(host, keyfile)
			go func() {
				defer func() { node.ssh = nil }()

				t := time.NewTicker(5 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-f.RPCContext.Stopper.ShouldStop():
						return
					case <-t.C:
						if _, _, err := c.Conn.SendRequest("keepalive@golang.org", true, nil); err != nil {
							// There's no useful response from these, so we
							// can just abort if there's an error.
							return
						}
					}
				}
			}()
			node.ssh = c
			return c, err
		}
	}
	return newSSH(host, keyfile)
}

func newSSH(host, keyfile string) (*ssh.Client, error) {
	privateKey, err := ioutil.ReadFile(keyfile)
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		return nil, err
	}
	return ssh.Dial("tcp", net.JoinHostPort(host, "22"), &ssh.ClientConfig{
		User: sshUser,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	})
}

func (f *Farmer) ssh(host, keyfile, cmd string) (string, string, error) {
	c, err := f.getSSH(host, keyfile)
	if err != nil {
		return "", "", err
	}
	s, err := c.NewSession()
	if err != nil {
		return "", "", err
	}
	defer s.Close()

	var outBuf, errBuf bytes.Buffer
	s.Stdout = &outBuf
	s.Stderr = &errBuf

	{
		err := s.Run(cmd)
		return outBuf.String(), errBuf.String(), err
	}
}
