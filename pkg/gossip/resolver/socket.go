// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolver

import (
	"net"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
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
	return nil, errors.Errorf("unknown address type: %q", sr.typ)
}
