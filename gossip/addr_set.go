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

package gossip

import (
	"math/rand"
	"net"
)

// Keeps a set of addresses and provides simple address-matched
// management functions. addrSet is not thread safe.
type addrSet struct {
	addrs   map[string]net.Addr // Map from net.Addr.String() to net.Addr
	maxSize int                 // Maximum size of set
}

func newAddrSet(maxSize int) *addrSet {
	return &addrSet{
		addrs:   make(map[string]net.Addr),
		maxSize: maxSize,
	}
}

// hasSpace returns whether there are fewer than maxSize addrs
// in the addrs array.
func (as *addrSet) hasSpace() bool {
	return len(as.addrs) < as.maxSize
}

// len returns the number of addresses in the set.
func (as *addrSet) len() int {
	return len(as.addrs)
}

// asSlice returns the addresses as a slice.
func (as *addrSet) asSlice() []net.Addr {
	arr := make([]net.Addr, len(as.addrs))
	var count int
	for _, addr := range as.addrs {
		arr[count] = addr
		count++
	}
	return arr
}

// selectRandom returns a random address from the set. Returns nil if
// there are no addresses to select.
func (as *addrSet) selectRandom() net.Addr {
	idx := rand.Intn(len(as.addrs))
	var count = 0
	for _, addr := range as.addrs {
		if count == idx {
			return addr
		}
		count++
	}
	return nil
}

// filter returns an addrSet of addresses which return true when
// passed to the supplied filter function filterFn. filterFn should
// return true to keep an address and false to remove an address.
func (as *addrSet) filter(filterFn func(addr net.Addr) bool) *addrSet {
	avail := newAddrSet(as.maxSize)
	for _, addr := range as.addrs {
		if filterFn(addr) {
			avail.addAddr(addr)
		}
	}
	return avail
}

// hasAddr verifies that the supplied address matches a addr
// in the array.
func (as *addrSet) hasAddr(addr net.Addr) bool {
	_, ok := as.addrs[addr.String()]
	return ok
}

// addAddr adds the addr to the addrs set.
func (as *addrSet) addAddr(addr net.Addr) {
	as.addrs[addr.String()] = addr
}

// removeAddr removes the addr from the addrs set.
func (as *addrSet) removeAddr(addr net.Addr) {
	delete(as.addrs, addr.String())
}
