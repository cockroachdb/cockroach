// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpcclientconnclose

import "github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/forbiddenmethod"

// Analyzer checks for calls to (*grpc.ClientConn).Close. We mostly pull these
// objects from *rpc.Context, which manages their lifecycle.
// Errant calls to Close() disrupt the connection for all users.
// (Exported from forbiddenmethod.)
var Analyzer = forbiddenmethod.GRPCClientConnCloseAnalyzer
