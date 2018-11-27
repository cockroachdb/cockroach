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

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// todoSpanSet is a placeholder value for callsites that need to pass a properly
// populated SpanSet (with according protection by the command queue) but fail
// to do so at the time of writing.
//
// See https://github.com/cockroachdb/cockroach/issues/19851.
//
// Do not introduce new uses of this.
var todoSpanSet = &spanset.SpanSet{}

// NewReplicaEvalContext returns a batcheval.EvalContext to use for command
// evaluation. The supplied SpanSet will be ignored except for race builds, in
// which case state access is asserted against it. A SpanSet must always be
// passed.
func NewReplicaEvalContext(r *Replica, ss *spanset.SpanSet) batcheval.EvalContext {
	if ss == nil {
		log.Fatalf(r.AnnotateCtx(context.Background()), "can't create a ReplicaEvalContext with assertions but no SpanSet")
	}
	if util.RaceEnabled {
		return &SpanSetReplicaEvalContext{
			i:  r,
			ss: *ss,
		}
	}
	return r
}
