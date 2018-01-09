// Copyright 2017 The Cockroach Authors.
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

package distsqlrun

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// MakeEvalContext serializes some of the fields of a tree.EvalContext into a
// distsqlrun.EvalContext proto.
func MakeEvalContext(evalCtx tree.EvalContext) EvalContext {
	return EvalContext{
		StmtTimestampNanos: evalCtx.GetStmtTimestamp().UnixNano(),
		TxnTimestampNanos:  evalCtx.GetTxnTimestampRaw().UnixNano(),
		ClusterTimestamp:   evalCtx.GetClusterTimestampRaw(),
		Location:           evalCtx.GetLocation().String(),
		Database:           evalCtx.SessionData.Database,
		User:               evalCtx.SessionData.User,
	}
}
