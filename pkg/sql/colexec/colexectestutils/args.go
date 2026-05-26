// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
