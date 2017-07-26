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
	"bytes"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/sftp"
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
	if len(f.nodes) == 0 {
		f.refresh()
	}
	for i := range f.nodes {
		node := &f.nodes[i]
		if node.hostname == host {
			if c := node.ssh; c != nil {
				return c, nil
			}
			c, err := newSSH(host, keyfile)
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
	conn, err := (&net.Dialer{
		KeepAlive: 5 * time.Second,
	}).Dial("tcp", net.JoinHostPort(host, "22"))
	if err != nil {
		return nil, err
	}
	c, chans, reqs, err := ssh.NewClientConn(conn, host, &ssh.ClientConfig{
		User: sshUser,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
	})
	if err != nil {
		return nil, err
	}
	return ssh.NewClient(c, chans, reqs), nil
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

	var stdout, stderr bytes.Buffer

	s.Stdout = &stdout
	s.Stderr = &stderr

	return stdout.String(), stderr.String(), s.Run(cmd)
}

func (f *Farmer) copyDir(host, keyfile, src, dest string) error {
	c, err := f.getSSH(host, keyfile)
	if err != nil {
		return err
	}

	sftp, err := sftp.NewClient(c)
	if err != nil {
		return err
	}
	defer sftp.Close()

	for w := sftp.Walk(src); w.Step(); {
		if err := w.Err(); err != nil {
			return err
		}
		if err := func() error {
			srcFile, err := sftp.Open(w.Path())
			if err != nil {
				return err
			}
			defer srcFile.Close()

			destFile, err := os.Create(filepath.Join(dest, srcFile.Name()))
			if err != nil {
				return err
			}
			defer destFile.Close()
			if _, err := io.Copy(destFile, srcFile); err != nil {
				return err
			}
			return destFile.Close()
		}(); err != nil {
			return err
		}
	}
	return nil
}
