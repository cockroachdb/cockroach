// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
)

// Tester is a proxy for e.g. testing.T which does not introduce a dependency
// on "testing".
type Tester interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

// tempUnixFile creates a temporary file for use with a unix domain socket.
// TODO(bdarnell): use TempDir instead to make this atomic.
func tempUnixFile() string {
	f, err := ioutil.TempFile("", "unix-socket")
	if err != nil {
		log.Fatalf("unable to create temp file: %s", err)
	}
	f.Close()
	if err := os.Remove(f.Name()); err != nil {
		log.Fatalf("unable to remove temp file: %s", err)
	}
	return f.Name()
}

// tempLocalhostAddr creates an address to localhost using a monotonically
// increasing port number in the range [minLocalhostPort, ...].
func tempLocalhostAddr() string {
	return "127.0.0.1:0"
}

// CreateTempDir creates a temporary directory and returns its path.
// You should usually call defer CleanupDir(dir) right after.
func CreateTempDir(t Tester, prefix string) string {
	dir, err := ioutil.TempDir("", prefix)
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// CreateNTempDirs creates N temporary directories and returns a slice
// of paths.
// You should usually call defer CleanupDirs(dirs) right after.
func CreateNTempDirs(t Tester, prefix string, n int) []string {
	dirs := make([]string, n)
	var err error
	for i := 0; i < n; i++ {
		dirs[i], err = ioutil.TempDir("", prefix)
		if err != nil {
			t.Fatal(err)
		}
	}
	return dirs
}

// CleanupDir removes the passed-in directory and all contents. Errors are ignored.
func CleanupDir(dir string) {
	_ = os.RemoveAll(dir)
}

// CleanupDirs removes all passed-in directories and their contents. Errors are ignored.
func CleanupDirs(dirs []string) {
	for _, dir := range dirs {
		_ = os.RemoveAll(dir)
	}
}

// CreateTestAddr creates an unused address for testing. The "network"
// parameter should be one of "tcp" or "unix".
func CreateTestAddr(network string) net.Addr {
	switch network {
	case "tcp":
		addr, err := net.ResolveTCPAddr("tcp", tempLocalhostAddr())
		if err != nil {
			panic(err)
		}
		return addr
	case "unix":
		addr, err := net.ResolveUnixAddr("unix", tempUnixFile())
		if err != nil {
			panic(err)
		}
		return addr
	}
	panic(fmt.Sprintf("unknown network type: %s", network))
}

// IsTrueWithin returns an error if the supplied function fails to
// evaluate to true within the specified duration. The function is
// invoked immediately at first and then successively with an
// exponential backoff starting at 1ns and ending at the specified
// duration.
//
// This method is deprecated; use SucceedsWithin instead.
// TODO(bdarnell): convert existing uses of IsTrueWithin to SucceedsWithin.
func IsTrueWithin(trueFunc func() bool, duration time.Duration) error {
	total := time.Duration(0)
	for wait := time.Duration(1); total < duration; wait *= 2 {
		if trueFunc() {
			return nil
		}
		time.Sleep(wait)
		total += wait
	}
	return fmt.Errorf("condition failed to evaluate true within %s", duration)
}

// SucceedsWithin fails the test (with t.Fatal) unless the supplied
// function runs without error within the specified duration. The
// function is invoked immediately at first and then successively with
// an exponential backoff starting at 1ns and ending at the specified
// duration.
func SucceedsWithin(t Tester, duration time.Duration, fn func() error) {
	total := time.Duration(0)
	var lastErr error
	for wait := time.Duration(1); total < duration; wait *= 2 {
		lastErr = fn()
		if lastErr == nil {
			return
		}
		time.Sleep(wait)
		total += wait
	}
	t.Fatalf("condition failed to evaluate within %s: %s", duration, lastErr)
}
