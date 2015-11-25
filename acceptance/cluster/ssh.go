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
	"bytes"
	"io"
	"io/ioutil"

	"golang.org/x/crypto/ssh"
)

func loadKey(file string) (ssh.AuthMethod, error) {
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	key, err := ssh.ParsePrivateKey(buffer)
	if err != nil {
		return nil, err
	}
	return ssh.PublicKeys(key), nil
}

func execute(user, hostport, keyfile, cmd string) (stdout string, stderr string, err error) {
	keyAuth, err := loadKey(keyfile)
	if err != nil {
		return
	}
	sshConfig := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{keyAuth},
	}

	conn, err := ssh.Dial("tcp", hostport, sshConfig)
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close()
	}()
	session, err := conn.NewSession()
	if err != nil {
		return
	}
	defer func() {
		_ = session.Close()
	}()

	stdoutPipe, err := session.StdoutPipe()
	if err != nil {
		return
	}

	stderrPipe, err := session.StderrPipe()
	if err != nil {
		return
	}

	err = session.Run(cmd)
	var errBuf, outBuf bytes.Buffer
	_, _ = io.Copy(&outBuf, stdoutPipe)
	_, _ = io.Copy(&errBuf, stderrPipe)
	stderr = errBuf.String()
	stdout = outBuf.String()
	return
}
