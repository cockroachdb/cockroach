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

// AddNode implements the scbuildstmt.BuilderState interface.
func (b *builderState) AddNode(
	status, targetStatus scpb.Status, elem scpb.Element, meta scpb.TargetMetadata,
) {
	for _, node := range b.output {
		if screl.EqualElements(node.Element(), elem) {
			panic(errors.AssertionFailedf("element already present in builder state: %s", elem))
		}
	}
	b.output = append(b.output, &scpb.Node{
		Target: scpb.NewTarget(targetStatus, elem, &meta),
		Status: status,
	})
}

// ForEachNode implements the scbuildstmt.BuilderState interface.
func (b *builderState) ForEachNode(fn func(status, targetStatus scpb.Status, elem scpb.Element)) {
	for _, node := range b.output {
		fn(node.Status, node.TargetStatus, node.Element())
	}
}
