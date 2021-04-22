// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexectestutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
)

// MakeInputs is a utility function that populates a slice of
// colexecargs.OpWithMetaInfo objects based on sources.
func MakeInputs(sources []colexecop.Operator) []colexecargs.OpWithMetaInfo {
	inputs := make([]colexecargs.OpWithMetaInfo, len(sources))
	for i := range sources {
		inputs[i].Root = sources[i]
	}
	return inputs
}
