// Copyright 2015 The Cockroach Authors.
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
// Author: Marc Berhault (marc@cockroachlabs.com)

package resolver

import (
	"net"

	"github.com/cockroachdb/cockroach/util"
)

// socketResolver represents the different types of socket-based
// address resolvers.
type socketResolver struct {
	typ  string
	addr string
}

// Type returns the resolver type.
func (sr *socketResolver) Type() string { return sr.typ }

// Addr returns the resolver address.
func (sr *socketResolver) Addr() string { return sr.addr }

// GetAddress returns a net.Addr or error.
func (sr *socketResolver) GetAddress() (net.Addr, error) {
	switch sr.typ {
	case "tcp":
		_, err := net.ResolveTCPAddr("tcp", sr.addr)
		if err != nil {
			return nil, err
		}
		return util.NewUnresolvedAddr("tcp", sr.addr), nil
	}
	return nil, util.Errorf("unknown address type: %q", sr.typ)
}
