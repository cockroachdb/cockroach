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
	dir scpb.Target_Direction, elem scpb.Element, meta scpb.TargetMetadata,
) {
	for _, node := range b.output {
		if screl.EqualElements(node.Element(), elem) {
			panic(errors.AssertionFailedf("element already present in builder state: %s", elem))
		}
	}
	b.output = append(b.output, &scpb.Node{
		Target: scpb.NewTarget(dir, elem, &meta),
		Status: nodeStatusFromDirection(dir),
	})
}

func nodeStatusFromDirection(dir scpb.Target_Direction) scpb.Status {
	switch dir {
	case scpb.Target_ADD:
		return scpb.Status_ABSENT
	case scpb.Target_DROP:
		return scpb.Status_PUBLIC
	default:
		panic(errors.AssertionFailedf("unknown direction %s", dir))
	}
}

// ForEachNode implements the scbuildstmt.BuilderState interface.
func (b *builderState) ForEachNode(
	fn func(status scpb.Status, dir scpb.Target_Direction, elem scpb.Element),
) {
	for _, node := range b.output {
		fn(node.Status, node.Direction, node.Element())
	}
}
