// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

var _ scbuildstmt.BuilderState = (*builderState)(nil)

// AddElementStatus implements the scbuildstmt.BuilderState interface.
func (b *builderState) AddElementStatus(
	currentStatus, targetStatus scpb.Status, elem scpb.Element, meta scpb.TargetMetadata,
) {
	for _, e := range b.output {
		if screl.EqualElements(e.element, elem) {
			panic(errors.AssertionFailedf("element already present in builder state: %s", elem))
		}
	}
	b.output = append(b.output, elementState{
		element:       elem,
		targetStatus:  targetStatus,
		currentStatus: currentStatus,
		metadata:      meta,
	})
}

// ForEachElementStatus implements the scpb.ElementStatusIterator interface.
func (b *builderState) ForEachElementStatus(
	fn func(status, targetStatus scpb.Status, elem scpb.Element),
) {
	for _, es := range b.output {
		fn(es.currentStatus, es.targetStatus, es.element)
	}
}
