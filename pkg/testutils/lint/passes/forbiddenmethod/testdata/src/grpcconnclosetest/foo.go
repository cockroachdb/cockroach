// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpcconnclosetest

import "google.golang.org/grpc"

func F() {
	cc := &grpc.ClientConn{}
	cc.Close() // want `Illegal call to ClientConn\.Close\(\)`

	//nolint:grpcconnclose
	cc.Close()
}
