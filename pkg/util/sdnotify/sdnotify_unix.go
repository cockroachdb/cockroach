// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

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

func ready() error {
	return notifyEnv(readyMsg)
}

func notifyEnv(msg string) error {
	if path, ok := os.LookupEnv(envName); ok {
		return notify(path, msg)
	}
	return nil
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
	defer net.Conn(conn).Close()

	_, err = conn.Write([]byte(msg))
	return err
}

func bgExec(cmd *exec.Cmd) error {
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
	net.Conn(l.conn).Close()
	return os.RemoveAll(l.tempDir)
}
