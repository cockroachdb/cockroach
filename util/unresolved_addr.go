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
// Author: jqmp (jaqueramaphan@gmail.com)

// TODO(jqmp): Needs testing.

package util

// UnresolvedAddr is an unresolved version of net.Addr.
type UnresolvedAddr struct {
	// These fields are only exported so that gob can see them.
	NetworkField string `json:"network"`
	StringField  string `json:"string"`
}

// MakeUnresolvedAddr creates a new UnresolvedAddr from a network
// and raw address string.
func MakeUnresolvedAddr(network string, str string) UnresolvedAddr {
	return UnresolvedAddr{
		NetworkField: network,
		StringField:  str,
	}
}

// Network returns the address's network name.
func (a UnresolvedAddr) Network() string {
	return a.NetworkField
}

// String returns the address's string form.
func (a UnresolvedAddr) String() string {
	return a.StringField
}
