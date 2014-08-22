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
// Author: jqmp (jaqueramaphan@gmail.com)

package rpc

import (
	"testing"

	"github.com/cockroachdb/cockroach/util"
)

func checkUpdateMatches(t *testing.T, network, oldAddrString, newAddrString, expAddrString string) {
	oldAddr := util.MakeRawAddr(network, oldAddrString)
	newAddr := util.MakeRawAddr(network, newAddrString)
	expAddr := util.MakeRawAddr(network, expAddrString)

	retAddr, err := updatedAddr(oldAddr, newAddr)
	if err != nil {
		t.Fatalf("updatedAddr failed on %v, %v: %v", oldAddr, newAddr, err)
	}

	if retAddr.String() != expAddrString {
		t.Fatalf("updatedAddr(%v, %v) was %s; expected %s", oldAddr, newAddr, retAddr, expAddr)
	}
}

func checkUpdateFails(t *testing.T, network, oldAddrString, newAddrString string) {
	oldAddr := util.MakeRawAddr(network, oldAddrString)
	newAddr := util.MakeRawAddr(network, newAddrString)

	retAddr, err := updatedAddr(oldAddr, newAddr)
	if err == nil {
		t.Fatalf("updatedAddr(%v, %v) should have failed; instead returned %v", oldAddr, newAddr, retAddr)
	}
}

func TestUpdatedAddr(t *testing.T) {
	for _, network := range []string{"tcp", "tcp4", "tcp6"} {
		checkUpdateMatches(t, network, "localhost:0", "127.0.0.1:1234", "localhost:1234")
		checkUpdateMatches(t, network, "localhost:1234", "127.0.0.1:1234", "localhost:1234")
		// This case emits a warning, but doesn't fail.
		checkUpdateMatches(t, network, "localhost:1234", "127.0.0.1:1235", "localhost:1235")
	}

	checkUpdateMatches(t, "unix", "address", "address", "address")
	checkUpdateFails(t, "unix", "address", "anotheraddress")
}
