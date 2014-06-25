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
// implied.  See the License for the specific language governing
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
	"reflect"
	"time"

	"github.com/golang/glog"
)

// tempUnixFile creates a temporary file for use with a unix domain socket.
func tempUnixFile() string {
	f, err := ioutil.TempFile("", "unix-socket")
	if err != nil {
		glog.Fatalf("unable to create temp file: %s", err)
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

// ContainsSameElements compares, without taking order on the
// first level, the contents of two slices of the same type.
// The elements of the slice must have comparisons defined,
// otherwise a panic will result.
func ContainsSameElements(s1, s2 interface{}) bool {
	// TODO add support for chans when needed.
	// Preliminary checks to weed out incompatible inputs.
	if reflect.TypeOf(s1).Kind() != reflect.Slice || reflect.TypeOf(s2).Kind() != reflect.Slice {
		panic("Received non-slice argument")
	}
	if reflect.TypeOf(s1) != reflect.TypeOf(s2) {
		panic("Received slices with incompatible types")
	}

	// Create a map that keeps a count of how often we see
	// each element in the slices.
	m := reflect.MakeMap(reflect.MapOf(reflect.TypeOf(s1).Elem(), reflect.TypeOf(uint64(0))))

	// Get the actual slices we are comparing.
	rs1 := reflect.ValueOf(s1)
	rs2 := reflect.ValueOf(s2)

	var zeroValue reflect.Value

	if rs1.Len() != rs2.Len() {
		return false
	}

	// Fill the counting hash map using the first slice.
	for i := 0; i < rs1.Len(); i++ {
		v := rs1.Index(i)
		k := m.MapIndex(v)
		if k == zeroValue {
			// The entry did not exist, so the new count is 1.
			m.SetMapIndex(v, reflect.ValueOf(uint64(1)))
		} else {
			m.SetMapIndex(v, reflect.ValueOf(k.Uint()+uint64(1)))
		}
	}

	// Compare the counts from s1 against the second slice.
	for i := 0; i < rs2.Len(); i++ {
		v := rs2.Index(i)
		k := m.MapIndex(v)
		if k == zeroValue {
			return false
		} else if k.Uint() == 1 {
			// Setting the zero value removes the entry.
			m.SetMapIndex(v, zeroValue)
		} else {
			m.SetMapIndex(v, reflect.ValueOf(k.Uint()-uint64(1)))
		}
	}

	// If all went well until here, the map is now empty.
	return m.Len() == 0
}
