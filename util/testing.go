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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
)

// Tester is a proxy for e.g. testing.T which does not introduce a dependency
// on "testing".
type Tester interface {
	Error(args ...interface{})
	Failed() bool
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

type panicTesterImpl struct{}

// PanicTester is a Tester which panics.
var PanicTester Tester

func (panicTesterImpl) Failed() bool { return false }

func (panicTesterImpl) Error(args ...interface{}) {
	panic(fmt.Sprint(args...))
}

func (pt panicTesterImpl) Fatal(args ...interface{}) {
	pt.Error(args...)
}

func (panicTesterImpl) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// tempUnixFile creates a temporary file for use with a unix domain socket.
// TODO(bdarnell): use TempDir instead to make this atomic.
func tempUnixFile() string {
	f, err := ioutil.TempFile("", "unix-socket")
	if err != nil {
		panic(fmt.Sprintf("unable to create temp file: %s", err))
	}
	f.Close()
	if err := os.Remove(f.Name()); err != nil {
		panic(fmt.Sprintf("unable to remove temp file: %s", err))
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

// CreateRestrictedFile creates a file on disk which contains the
// supplied byte string as its content. The resulting file will have restrictive
// permissions; specifically, u=rw (0600). Returns the path of the created file
// along with a function that will delete the created file.
//
// This is needed for some Go libraries (e.g. postgres SQL driver) which will
// refuse to open certificate files that have overly permissive permissions.
func CreateRestrictedFile(t Tester, contents []byte, tempdir, name string) string {
	tempPath := filepath.Join(tempdir, name)
	if err := ioutil.WriteFile(tempPath, contents, 0600); err != nil {
		if t == nil {
			log.Fatal(err)
		} else {
			t.Fatal(err)
		}
	}
	return tempPath
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

const defaultSucceedsSoonDuration = 15 * time.Second

// SucceedsSoon fails the test (with t.Fatal) unless the supplied
// function runs without error within a preset maximum duration. The
// function is invoked immediately at first and then successively with
// an exponential backoff starting at 1ns and ending at the maximum
// duration (currently 15s).
func SucceedsSoon(t Tester, fn func() error) {
	SucceedsSoonDepth(1, t, fn)
}

// SucceedsSoonDepth is like SucceedsSoon() but with an additional
// stack depth offset.
func SucceedsSoonDepth(depth int, t Tester, fn func() error) {
	if err := RetryForDuration(defaultSucceedsSoonDuration, fn); err != nil {
		t.Fatal(ErrorfSkipFrames(1+depth, "condition failed to evaluate within %s: %s", defaultSucceedsSoonDuration, err))
	}
}

// RetryForDuration will retry the given function until it either returns
// without error, or the given duration has elapsed. The function is invoked
// immediately at first and then successively with an exponential backoff
// starting at 1ns and ending at the specified duration.
func RetryForDuration(duration time.Duration, fn func() error) error {
	deadline := time.Now().Add(duration)
	var lastErr error
	for wait := time.Duration(1); time.Now().Before(deadline); wait *= 2 {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if wait > time.Second {
			wait = time.Second
		}
		time.Sleep(wait)
	}
	return lastErr
}

// Panics calls the supplied function and returns true if and only if it panics.
func Panics(f func()) (panics bool) {
	defer func() {
		panics = recover() != nil
	}()
	f()
	return
}

// NoZeroField returns nil if none of the fields of the struct underlying the
// interface are equal to the zero value, and an error otherwise.
// It will panic if the struct has unexported fields and for any non-struct.
func NoZeroField(v interface{}) error {
	ele := reflect.Indirect(reflect.ValueOf(v))
	eleT := ele.Type()
	for i := 0; i < ele.NumField(); i++ {
		f := ele.Field(i)
		zero := reflect.Zero(f.Type())
		if reflect.DeepEqual(f.Interface(), zero.Interface()) {
			return fmt.Errorf("expected %s field to be non-zero", eleT.Field(i).Name)
		}
	}
	return nil
}
