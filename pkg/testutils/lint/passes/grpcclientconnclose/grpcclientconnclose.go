// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package grpcclientconnclose

import "github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/forbiddenmethod"

// Analyzer checks for calls to (*grpc.ClientConn).Close. We mostly pull these
// objects from *rpc.Context, which manages their lifecycle.
// Errant calls to Close() disrupt the connection for all users.
// (Exported from forbiddenmethod.)
var Analyzer = forbiddenmethod.GRPCClientConnCloseAnalyzer
