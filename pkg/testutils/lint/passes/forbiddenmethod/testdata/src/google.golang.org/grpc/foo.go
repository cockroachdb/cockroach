// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpc

type ClientConn struct{}

func (*ClientConn) Close() {}
