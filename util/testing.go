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

// tempUnixFile creates a temporary file for use with a unix domain socket.
func tempUnixFile() string {
	f, err := ioutil.TempFile("", "unix-socket")
	if err != nil {
		log.Fatalf("unable to create temp file: %s", err)
	}
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

// tempLocalhostAddr creates an address to localhost using a monotonically
// increasing port number in the range [minLocalhostPort, ...].
func tempLocalhostAddr() string {
	return "127.0.0.1:0"
}

// CreateTempDirectory creates a temporary directory or fails trying.
func CreateTempDirectory() string {
	loc, err := ioutil.TempDir("", "rocksdb_test")
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}
	return loc
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
// invoked at most 10 times over the course of the specified time
// duration.
func IsTrueWithin(trueFunc func() bool, duration time.Duration) error {
	for i := 0; i < 10; i++ {
		if trueFunc() {
			return nil
		}
		time.Sleep(duration / 10)
	}
	return fmt.Errorf("condition failed to evaluate true within %s", duration)
}
