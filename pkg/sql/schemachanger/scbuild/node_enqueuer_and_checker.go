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
)

var _ scbuildstmt.NodeEnqueuerAndChecker = buildCtx{}

// HasNode implements the scbuildstmt.NodeEnqueuerAndChecker interface.
func (b buildCtx) HasNode(
	filter func(status scpb.Status, dir scpb.Target_Direction, elem scpb.Element) bool,
) (found bool) {
	b.ForEachNode(func(status scpb.Status, dir scpb.Target_Direction, elem scpb.Element) {
		if filter(status, dir, elem) {
			found = true
		}
	})
	return found
}

// HasTarget implements the scbuildstmt.NodeEnqueuerAndChecker interface.
func (b buildCtx) HasTarget(dir scpb.Target_Direction, elem scpb.Element) (found bool) {
	return b.HasNode(func(_ scpb.Status, d scpb.Target_Direction, e scpb.Element) bool {
		return d == dir && screl.EqualElements(e, elem)
	})
}

// HasElement implements the scbuildstmt.NodeEnqueuerAndChecker interface.
func (b buildCtx) HasElement(elem scpb.Element) bool {
	return b.HasNode(func(_ scpb.Status, d scpb.Target_Direction, e scpb.Element) bool {
		return screl.EqualElements(e, elem)
	})
}

// EnqueueAdd implements the scbuildstmt.NodeEnqueuerAndChecker interface.
func (b buildCtx) EnqueueAdd(elem scpb.Element) {
	b.AddNode(scpb.Target_ADD, elem, b.TargetMetadata())
}

// EnqueueDrop implements the scbuildstmt.NodeEnqueuerAndChecker interface.
func (b buildCtx) EnqueueDrop(elem scpb.Element) {
	b.AddNode(scpb.Target_DROP, elem, b.TargetMetadata())
}

// EnqueueDropIfNotExists implements the scbuildstmt.NodeEnqueuerAndChecker
// interface.
func (b buildCtx) EnqueueDropIfNotExists(elem scpb.Element) {
	if b.HasTarget(scpb.Target_DROP, elem) {
		return
	}
	b.AddNode(scpb.Target_DROP, elem, b.TargetMetadata())
}
