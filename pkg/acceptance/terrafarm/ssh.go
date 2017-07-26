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
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
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

func (f *Farmer) scp(host, keyfile, src, dest string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	srcFileInfo, err := srcFile.Stat()
	if err != nil {
		return err
	}

	c, err := f.getSSH(host, keyfile)
	if err != nil {
		return err
	}
	s, err := c.NewSession()
	if err != nil {
		return err
	}
	defer s.Close()

	w, err := s.StdinPipe()
	if err != nil {
		return err
	}
	defer w.Close()

	dir, file := filepath.Split(dest)

	if err := s.Start("/usr/bin/scp -t " + dir); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "C", srcFileInfo.Mode().String(), srcFileInfo.Size(), file); err != nil {
		return err
	}
	if _, err := io.Copy(w, srcFile); err != nil {
		return err
	}
	if _, err := w.Write([]byte{0}); err != nil {
		return err
	}

	return s.Wait()
}
