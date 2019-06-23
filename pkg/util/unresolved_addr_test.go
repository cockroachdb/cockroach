// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"net"
	"testing"
)

func TestUnresolvedAddr(t *testing.T) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:12345")
	if err != nil {
		t.Fatal(err)
	}
	addr := MakeUnresolvedAddr(tcpAddr.Network(), tcpAddr.String())
	tcpAddr2, err := addr.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if tcpAddr2.Network() != tcpAddr.Network() {
		t.Errorf("networks differ: %s != %s", tcpAddr2.Network(), tcpAddr.Network())
	}
	if tcpAddr2.String() != tcpAddr.String() {
		t.Errorf("strings differ: %s != %s", tcpAddr2.String(), tcpAddr.String())
	}
}
