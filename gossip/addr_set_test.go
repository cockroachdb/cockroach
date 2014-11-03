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

package gossip

import (
	"net"
	"testing"
)

func TestMaxSize(t *testing.T) {
	addrs := newAddrSet(1)
	if !addrs.hasSpace() {
		t.Error("set should have space")
	}
	addrs.addAddr(emptyAddr)
	if addrs.hasSpace() {
		t.Error("set should have no space")
	}
}

func TestHasAddr(t *testing.T) {
	addrs := newAddrSet(2)
	addr := testAddr("<test-addr:0>")
	if addrs.hasAddr(addr) {
		t.Error("addr wasn't added and should not be valid")
	}
	// Add address and verify it's valid.
	addrs.addAddr(addr)
	if !addrs.hasAddr(addr) {
		t.Error("empty addr wasn't added and should not be valid")
	}
	// Try another object, same address.
	if !addrs.hasAddr(testAddr("<test-addr:0>")) {
		t.Error("empty addr wasn't added and should not be valid")
	}
}

func TestAddAndRemoveAddr(t *testing.T) {
	addrs := newAddrSet(2)
	addr0 := testAddr("<test-addr:0>")
	addr1 := testAddr("<test-addr:1>")
	addrs.addAddr(addr0)
	addrs.addAddr(addr1)
	if !addrs.hasAddr(addr0) || !addrs.hasAddr(addr1) {
		t.Error("failed to locate added addresses")
	}
	addrs.removeAddr(addr0)
	if addrs.hasAddr(addr0) || !addrs.hasAddr(addr1) {
		t.Error("failed to remove addr0", addrs)
	}
	addrs.removeAddr(addr1)
	if addrs.hasAddr(addr0) || addrs.hasAddr(addr1) {
		t.Error("failed to remove addr1", addrs)
	}
}

func TestAddrSetFilter(t *testing.T) {
	addrs1 := newAddrSet(2)
	addr0 := testAddr("<test-addr:0>")
	addr1 := testAddr("<test-addr:1>")
	addrs1.addAddr(addr0)
	addrs1.addAddr(addr1)

	addrs2 := newAddrSet(1)
	addrs2.addAddr(addr1)

	filtered := addrs1.filter(func(a net.Addr) bool {
		return !addrs2.hasAddr(a)
	})
	if filtered.len() != 1 || filtered.hasAddr(addr1) || !filtered.hasAddr(addr0) {
		t.Errorf("expected filter to leave addr0: %+v", filtered)
	}
}

func TestAddrSetAsSlice(t *testing.T) {
	addrs := newAddrSet(2)
	addr0 := testAddr("<test-addr:0>")
	addr1 := testAddr("<test-addr:1>")
	addrs.addAddr(addr0)
	addrs.addAddr(addr1)

	addrArr := addrs.asSlice()
	if len(addrArr) != 2 {
		t.Error("expected slice of length 2:", addrArr)
	}
	if (addrArr[0] != addr0 && addrArr[0] != addr1) ||
		(addrArr[1] != addr1 && addrArr[1] != addr0) {
		t.Error("expected slice to contain both addr0 and addr1:", addrArr)
	}
}
