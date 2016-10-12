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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

// Package sdnotify implements both sides of the systemd readiness
// protocol. Servers can use sdnotify.Ready() to signal that they are
// ready to receive traffic, and process managers can use
// sdnotify.Exec() to run processes that implement this protocol.
package sdnotify

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	envName  = "NOTIFY_SOCKET"
	readyMsg = "READY=1"
	netType  = "unixgram"
)

// Ready sends a readiness signal using the systemd notification
// protocol. It should be called (once) by a server after it has
// completed its initialization (including but not necessarily limited
// to binding ports) and is ready to receive traffic.
func Ready() error {
	return notifyEnv(readyMsg)
}

func notifyEnv(msg string) error {
	path := os.Getenv(envName)
	if path == "" {
		return nil
	}
	return notify(path, msg)
}

func notify(path, msg string) error {
	addr := net.UnixAddr{
		Net:  netType,
		Name: path,
	}
	conn, err := net.DialUnix(netType, nil, &addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Write([]byte(msg))
	return err
}

// Exec the given command in the background using the systemd
// notification protocol. This function returns once the command has
// either exited or signaled that it is ready. If the command exits
// with a non-zero status before signaling readiness, returns an
// exec.ExitError.
func Exec(cmd *exec.Cmd) error {
	l, err := listen()
	if err != nil {
		return err
	}
	defer func() { _ = l.close() }()

	if cmd.Env == nil {
		// Default the environment to the parent process's, minus any
		// existing versions of our variable.
		varPrefix := fmt.Sprintf("%s=", envName)
		for _, v := range os.Environ() {
			if !strings.HasPrefix(v, varPrefix) {
				cmd.Env = append(cmd.Env, v)
			}
		}
	}
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envName, l.Path))

	if err := cmd.Start(); err != nil {
		return err
	}

	// This can leak goroutines, but we don't really care because we
	// always exit after calling this function.
	ch := make(chan error, 2)
	go func() {
		ch <- l.wait()
	}()
	go func() {
		ch <- cmd.Wait()
	}()
	return <-ch
}

type listener struct {
	Path string

	tempDir string
	conn    *net.UnixConn
}

func listen() (listener, error) {
	dir, err := ioutil.TempDir("", "sdnotify")
	if err != nil {
		return listener{}, err
	}

	path := filepath.Join(dir, "notify.sock")
	conn, err := net.ListenUnixgram(netType, &net.UnixAddr{
		Net:  netType,
		Name: path,
	})
	if err != nil {
		return listener{}, err
	}
	l := listener{
		Path:    path,
		tempDir: dir,
		conn:    conn}
	return l, nil
}

func (l listener) wait() error {
	buf := make([]byte, len(readyMsg))
	for {
		n, err := l.conn.Read(buf)
		if err != nil {
			return err
		}
		if string(buf[:n]) == readyMsg {
			return nil
		}
	}
}

func (l listener) close() error {
	l.conn.Close()
	return os.RemoveAll(l.tempDir)
}
