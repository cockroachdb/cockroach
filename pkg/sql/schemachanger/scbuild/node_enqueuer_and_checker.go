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
	filter func(status, targetStatus scpb.Status, elem scpb.Element) bool,
) (found bool) {
	b.ForEachNode(func(status, targetStatus scpb.Status, elem scpb.Element) {
		if filter(status, targetStatus, elem) {
			found = true
		}
	})
	return found
}

// HasTarget implements the scbuildstmt.NodeEnqueuerAndChecker interface.
func (b buildCtx) HasTarget(targetStatus scpb.Status, elem scpb.Element) (found bool) {
	return b.HasNode(func(_, ts scpb.Status, e scpb.Element) bool {
		return ts == targetStatus && screl.EqualElements(e, elem)
	})
}

// HasElement implements the scbuildstmt.NodeEnqueuerAndChecker interface.
func (b buildCtx) HasElement(elem scpb.Element) bool {
	return b.HasNode(func(_, _ scpb.Status, e scpb.Element) bool {
		return screl.EqualElements(e, elem)
	})
}

// EnqueueAdd implements the scbuildstmt.NodeEnqueuerAndChecker interface.
func (b buildCtx) EnqueueAdd(elem scpb.Element) {
	b.AddNode(scpb.Status_ABSENT, scpb.Status_PUBLIC, elem, b.TargetMetadata())
}

// EnqueueDrop implements the scbuildstmt.NodeEnqueuerAndChecker interface.
func (b buildCtx) EnqueueDrop(elem scpb.Element) {
	b.AddNode(scpb.Status_PUBLIC, scpb.Status_ABSENT, elem, b.TargetMetadata())
}

// EnqueueDropIfNotExists implements the scbuildstmt.NodeEnqueuerAndChecker
// interface.
func (b buildCtx) EnqueueDropIfNotExists(elem scpb.Element) {
	if b.HasTarget(scpb.Status_ABSENT, elem) {
		return
	}
	b.AddNode(scpb.Status_PUBLIC, scpb.Status_ABSENT, elem, b.TargetMetadata())
}
